import uuid
import zipfile
from typing import Dict, Optional, List, Generator

from apollo.agent.utils import AgentUtils
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.git.git_client import GitCloneClient, GitFileData
from apollo.integrations.storage.storage_proxy_client import StorageProxyClient

ZIP_FILE_EXPIRATION = 15 * 60  # 15 minutes


class GitProxyClient(BaseProxyClient):
    def __init__(self, credentials: Optional[Dict], platform: str, **kwargs):  # type: ignore
        if not credentials:
            raise ValueError("Credentials are required for Git")
        self._platform = platform
        self._client = GitCloneClient(credentials=credentials)

    @property
    def wrapped_client(self):
        return self._client

    def get_files(self, file_extensions: List[str]) -> str:
        files = self._client.get_files(file_extensions)
        zip_file_path = self._zip_file(files)
        storage_client = StorageProxyClient(self._platform)

        key = f"/tmp/{uuid.uuid4()}.zip"
        storage_client.upload_file(key, zip_file_path)
        return storage_client.generate_presigned_url(key, ZIP_FILE_EXPIRATION)

    @staticmethod
    def _zip_file(files: Generator[GitFileData, None, None]) -> str:
        tmp_path = AgentUtils.temp_file_path()
        with zipfile.ZipFile(tmp_path, mode="w") as zf:
            for file in files:
                zf.writestr(file.name, file.content)
        return tmp_path
