from typing import Dict, Optional

from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.git.git_client import GitCloneClientWrapper


class GitProxyClient(BaseProxyClient):
    def __init__(self, credentials: Optional[Dict], **kwargs):  # type: ignore
        if not credentials:
            raise ValueError("Credentials are required for Git")
        self._client = GitCloneClientWrapper(credentials=credentials)

    @property
    def wrapped_client(self):
        return self._client
