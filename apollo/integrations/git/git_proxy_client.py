import os
import uuid
import zipfile
from typing import Dict, Optional, List, Generator

from apollo.common.agent.utils import AgentUtils
from apollo.common.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.git.git_client import GitCloneClient, GitFileData
from apollo.integrations.storage.storage_proxy_client import StorageProxyClient

ZIP_FILE_EXPIRATION = 15 * 60  # 15 minutes


class GitProxyClient(BaseProxyClient):
    """
    Git Clone Proxy Client, clones the requested repo, uploads a zip file with its contents to the associated bucket
    and returns a pre-signed url to download it.
    """

    def __init__(self, credentials: Optional[Dict], platform: str, **kwargs):  # type: ignore
        """
        Credentials are expected to include:
        - repo_url
        - ssh_key
        - username (if ssh_key not specified)
        - token (if ssh_key not specified)
        """
        if not credentials:
            raise ValueError("Credentials are required for Git")
        self._platform = platform
        self._client = GitCloneClient(credentials=credentials)

    @property
    def wrapped_client(self):
        return self._client

    def get_files(self, file_extensions: List[str]) -> Dict:
        """
        Clones the repo, filters the files with the given extensions, uploads a zip file to the associated bucket and
        returns a pre-signed url to download it.
        :param file_extensions: a list of file extensions to filter the repository contents.
        :return: a dictionary with two keys: `key` with the path to the file in the bucket and `url` with the
            pre-signed url to download it.
        """
        files = self._client.get_files(file_extensions)
        zip_file_path = self._zip_file(files)
        storage_client = StorageProxyClient(self._platform)

        key = f"tmp/{uuid.uuid4()}.zip"
        storage_client.upload_file(key, zip_file_path)
        url = storage_client.generate_presigned_url(key, ZIP_FILE_EXPIRATION)
        os.remove(zip_file_path)
        return {"key": key, "url": url}

    @staticmethod
    def _zip_file(files: Generator[GitFileData, None, None]) -> str:
        tmp_path = AgentUtils.temp_file_path()
        with zipfile.ZipFile(tmp_path, mode="w") as zf:
            for file in files:
                zf.writestr(file.name, file.content)
        return tmp_path
