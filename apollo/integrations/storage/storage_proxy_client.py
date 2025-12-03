import os
from datetime import timedelta
from typing import (
    Optional,
    BinaryIO,
    Dict,
    Union,
    cast,
    Any,
)

from apollo.agent.constants import (
    PLATFORM_AZURE,
    PLATFORM_GCP,
    PLATFORM_AWS,
    PLATFORM_AWS_GENERIC,
    STORAGE_TYPE_AZURE,
    STORAGE_TYPE_GCS,
    STORAGE_TYPE_S3,
    STORAGE_TYPE_MINIO,
)
from apollo.agent.env_vars import (
    STORAGE_TYPE_ENV_VAR,
    STORAGE_PREFIX_ENV_VAR,
    STORAGE_PREFIX_DEFAULT_VALUE,
)
from apollo.agent.models import AgentConfigurationError, AgentOperation
from apollo.agent.redact import AgentRedactUtilities
from apollo.agent.utils import AgentUtils
from apollo.integrations.azure_blob.azure_blob_reader_writer import (
    AzureBlobReaderWriter,
)
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.gcs.gcs_reader_writer import GcsReaderWriter
from apollo.integrations.minio.minio_reader_writer import MinIOReaderWriter
from apollo.integrations.s3.s3_reader_writer import S3ReaderWriter
from apollo.integrations.storage.base_storage_client import BaseStorageClient

_API_SERVICE_NAME = "storage"
_API_VERSION = "v1"

_ERROR_TYPE_NOTFOUND = "NotFound"
_ERROR_TYPE_PERMISSIONS = "Permissions"

_BUCKET_NAME_LOG_ATTRIBUTE = "bucket_name"
_OBJ_TO_WRITE_ARG_NAME = "obj_to_write"

_DEFAULT_PLATFORM_STORAGE = {
    PLATFORM_AZURE: STORAGE_TYPE_AZURE,
    PLATFORM_GCP: STORAGE_TYPE_GCS,
    PLATFORM_AWS: STORAGE_TYPE_S3,
    PLATFORM_AWS_GENERIC: STORAGE_TYPE_S3,
}

_STORAGE_CLIENTS = {
    STORAGE_TYPE_AZURE: AzureBlobReaderWriter,
    STORAGE_TYPE_GCS: GcsReaderWriter,
    STORAGE_TYPE_S3: S3ReaderWriter,
    STORAGE_TYPE_MINIO: MinIOReaderWriter,
}


class StorageProxyClient(BaseProxyClient):
    """
    Proxy client for storage operations, it forwards calls to a `BaseStorageClient`, for example GCS, S3, or MinIO.
    The storage client to use is automatically derived from the platform:
    - AWS -> S3
    - GCP -> GCS
    - Generic -> S3/GCS/MinIO as configured by MCD_STORAGE env var
    Credentials to use by the storage client are derived from the environment, in the case of S3 from env vars as
    supported by boto3, for GCS from ADC (Application Default Credentials) that are automatically set when
    running in CloudRun and can be set with `gcloud` CLI or API in other cases. For MinIO, credentials are
    provided via environment variables.
    """

    def __init__(self, platform: str, **kwargs):  # type: ignore
        storage: Optional[str] = os.getenv(STORAGE_TYPE_ENV_VAR)
        if not storage:
            storage = _DEFAULT_PLATFORM_STORAGE.get(platform)
            if not storage:
                raise ValueError(f"Missing {STORAGE_TYPE_ENV_VAR} env var")

        prefix: Optional[str] = os.getenv(
            STORAGE_PREFIX_ENV_VAR, STORAGE_PREFIX_DEFAULT_VALUE
        )
        if prefix == "" or prefix == "/":
            prefix = None
        storage_client = _STORAGE_CLIENTS.get(storage)
        if not storage_client:
            raise AgentConfigurationError(f"Invalid storage type: {storage}")

        self._client = cast(BaseStorageClient, storage_client(prefix=prefix))

    @property
    def wrapped_client(self):
        return self._client

    def get_error_type(self, error: Exception) -> Optional[str]:
        """
        Returns an error type string for the given exception, this is used client side to create again the required
        exception type.
        :param error: the exception occurred.
        :return: an error type if the exception is mapped to an error type for this client, `None` otherwise.
        """
        if isinstance(error, BaseStorageClient.PermissionsError):
            return _ERROR_TYPE_PERMISSIONS
        elif isinstance(error, BaseStorageClient.NotFoundError):
            return _ERROR_TYPE_NOTFOUND
        return super().get_error_type(error)

    def log_payload(self, operation: AgentOperation) -> Dict:
        """
        Implements `log_payload` from `BaseProxyClient` to include the bucket name in log messages for this client.
        """
        payload: Dict[str, Any] = {
            **super().log_payload(operation),
            "bucket_name": self._client.bucket_name,
        }
        return AgentRedactUtilities.redact_attributes(payload, [_OBJ_TO_WRITE_ARG_NAME])

    def download_file(self, key: str) -> BinaryIO:
        """
        Downloads the file to a temporary file and returns a BinaryIO object with the contents.
        :param key: path to the file in the bucket
        :return: BinaryIO object with the contents of the file.
        """
        path = AgentUtils.temp_file_path()
        self._client.download_file(key, path)
        return AgentUtils.open_file(path)

    def upload_file(self, key: str, local_file_path: str):
        """
        Uploads the local file at `local_file_path` to `key` in the associated bucket
        :param key: target path in the bucket for the uploaded file
        :param local_file_path: local path of the file to upload.
        """
        self._client.upload_file(key, local_file_path)

    def write(self, key: str, obj_to_write: Union[bytes, str]):
        self._client.write(key, obj_to_write)

    def managed_download(self, key: str) -> BinaryIO:
        """
        Downloads the file to a temporary file and returns a BinaryIO object with the contents.
        :param key: path to the file in the bucket.
        :return: BinaryIO object with the contents of the file.
        """
        path = AgentUtils.temp_file_path()
        self._client.managed_download(key, path)
        return AgentUtils.open_file(path)

    def list_objects(self, *args, **kwargs):  # type: ignore
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
        :param key: path to the file in the bucket
        """
        return self._client.generate_presigned_url(
            key=key, expiration=timedelta(seconds=expiration)
        )

    def should_log_exception(self, ex: Exception) -> bool:
        """
        Don't log NotFound exceptions to reduce the number of error logs, dc-core checks if an idempotent
        request is present for every single request, and it always fails the first time.
        :param ex: the exception occurred.
        :return: False if the exception is a NotFound error, True otherwise.
        """
        if isinstance(ex, BaseStorageClient.NotFoundError):
            return False
        else:
            return super().should_log_exception(ex)
