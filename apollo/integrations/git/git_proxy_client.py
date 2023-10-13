from typing import Dict, Optional, List

from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.git.git_client import GitCloneClient


class GitProxyClient(BaseProxyClient):
    def __init__(self, credentials: Optional[Dict], **kwargs):  # type: ignore
        if not credentials:
            raise ValueError("Credentials are required for Git")
        self._client = GitCloneClient(credentials=credentials)

    @property
    def wrapped_client(self):
        return self._client

    def get_files(self, file_extensions: List[str]) -> List[Dict]:
        return list(self._client.get_files(file_extensions))
