import os
from datetime import timedelta
from typing import Optional, BinaryIO

from apollo.agent.constants import (
    PLATFORM_GCP,
    PLATFORM_GENERIC,
    PLATFORM_AWS,
    STORAGE_TYPE_GCS,
    STORAGE_TYPE_S3,
    STORAGE_TYPE_ENV_VAR,
)
from apollo.agent.utils import AgentUtils
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.gcs.gcs_reader_writer import GcsReaderWriter
from apollo.integrations.s3.s3_reader_writer import S3ReaderWriter
from apollo.integrations.storage.base_storage_client import BaseStorageClient

_API_SERVICE_NAME = "storage"
_API_VERSION = "v1"

_ERROR_TYPE_NOTFOUND = "NotFound"
_ERROR_TYPE_PERMISSIONS = "Permissions"


class StorageProxyClient(BaseProxyClient):
    """
    Proxy client for storage operations, it forwards calls to a `BaseStorageClient`, for example GCS or S3.
    The storage client to use is automatically derived from the platform:
    - AWS -> S3
    - GCP -> GCS
    - Generic -> S3/GCS as configured by MCD_STORAGE env var
    Credentials to use by the storage client are derived from the environment, in the case of S3 from env vars as
    supported by boto3, for GCS from ADC (Application Default Credentials) that are automatically set when
    running in CloudRun and can be set with `gcloud` CLI or API in other cases.
    """

    def __init__(self, platform: str, **kwargs):
        storage: Optional[str] = None
        if platform == PLATFORM_GCP:
            storage = STORAGE_TYPE_GCS
        elif platform == PLATFORM_AWS:
            storage = STORAGE_TYPE_S3
        elif platform == PLATFORM_GENERIC:
            storage = os.getenv(STORAGE_TYPE_ENV_VAR)
            if not storage:
                raise ValueError(f"Missing {STORAGE_TYPE_ENV_VAR} env var")

        if storage == STORAGE_TYPE_GCS:
            self._client = GcsReaderWriter()
        elif storage == STORAGE_TYPE_S3:
            self._client = S3ReaderWriter()
        else:
            raise ValueError(f"Invalid storage type: {storage}")

    @property
    def wrapped_client(self):
        return self._client

    def get_error_type(self, error: Exception) -> Optional[str]:
        """
        Returns an error type string for the given exception, this is used client side to create again the required
        exception type.
        """
        if isinstance(error, BaseStorageClient.PermissionsError):
            return _ERROR_TYPE_PERMISSIONS
        elif isinstance(error, BaseStorageClient.NotFoundError):
            return _ERROR_TYPE_NOTFOUND
        return super().get_error_type(error)

    def download_file(self, key: str) -> BinaryIO:
        """
        Downloads the file to a temporary file and returns a BinaryIO object with the contents
        """
        path = AgentUtils.temp_file_path()
        self._client.download_file(key, path)
        return AgentUtils.open_file(path)

    def managed_download(self, key: str) -> BinaryIO:
        """
        Downloads the file to a temporary file and returns a BinaryIO object with the contents
        """
        path = AgentUtils.temp_file_path()
        self._client.managed_download(key, path)
        return AgentUtils.open_file(path)

    def list_objects(self, *args, **kwargs):
        """
        Returns the list of objects and the continuation token, the tuple (list, token) returned by the storage
        client is converted to a dictionary with keys "list" and "page_token" so it can be serialized back
        as a JSON document.
        """
        result, page_token = self._client.list_objects(*args, **kwargs)
        return {
            "list": result,
            "page_token": page_token,
        }

    def generate_presigned_url(self, key: str, expiration: int) -> str:
        """
        Generates a pre-signed URL, converts the received expiration seconds to timedelta as that's the
        parameter type required by the storage client.
        """
        return self._client.generate_presigned_url(
            key=key, expiration=timedelta(seconds=expiration)
        )