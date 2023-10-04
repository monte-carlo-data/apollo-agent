from typing import Optional, BinaryIO

from apollo.agent.utils import AgentUtils
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.gcs.reader_writer import GcsReaderWriter
from apollo.integrations.storage.base_storage_client import BaseStorageClient

_API_SERVICE_NAME = "storage"
_API_VERSION = "v1"

_ERROR_TYPE_NOTFOUND = "NotFound"
_ERROR_TYPE_PERMISSIONS = "Permissions"


class StorageProxyClient(BaseProxyClient):
    def __init__(self, **kwargs):
        self._client: BaseStorageClient = GcsReaderWriter(credentials=None)

    @property
    def wrapped_client(self):
        return self._client

    def get_error_type(self, error: Exception) -> Optional[str]:
        if isinstance(error, BaseStorageClient.PermissionsError):
            return _ERROR_TYPE_PERMISSIONS
        elif isinstance(error, BaseStorageClient.NotFoundError):
            return _ERROR_TYPE_NOTFOUND
        return super().get_error_type(error)

    def download_file(self, key: str) -> BinaryIO:
        path = AgentUtils.temp_file_path()
        self._client.download_file(key, path)
        return AgentUtils.open_file(path)

    def managed_download(self, key: str) -> BinaryIO:
        path = AgentUtils.temp_file_path()
        self._client.managed_download(key, path)
        return AgentUtils.open_file(path)

    def list_objects(self, *args, **kwargs):
        print(f"list_objects args: {args}")
        print(f"list_objects kwargs: {kwargs}")
        result, page_token = self._client.list_objects(*args, **kwargs)
        return {
            "list": result,
            "page_token": page_token,
        }
