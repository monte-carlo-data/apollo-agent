from typing import (
    Any,
    Dict,
    Optional,
)
from urllib.parse import urlparse

from apollo.integrations.http.http_proxy_client import HttpProxyClient
from apollo.integrations.powerbi.msal_auth import PowerBiTokenProvider


class PowerBiProxyClient(HttpProxyClient):
    """
    Power BI proxy client.

    Selects the MSAL token audience per request by destination host, so the same connection and
    credentials serve both the Power BI REST API (``api.powerbi.com``) and the Microsoft Fabric
    API (``api.fabric.microsoft.com`` — Gen2 CI/CD dataflows). Acquisition is delegated to
    :class:`PowerBiTokenProvider`, which mints and caches a token per scope; requests to any
    other host get no ``Authorization`` header.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):  # noqa
        if not credentials:
            raise ValueError("Credentials are required for PowerBI")
        super().__init__(credentials=credentials)
        # ``self._credentials`` is the resolved ``connect_args`` (raw MSAL params in the normal
        # path, or a pre-shaped token in the legacy path) — see HttpProxyClient.__init__.
        self._token_provider = PowerBiTokenProvider(self._credentials)

    def _attach_auth_header(self, headers: Dict, url: Optional[str] = None) -> None:
        host = urlparse(url).hostname if url else None
        token = self._token_provider.token_for_host(host)
        if token:
            headers["Authorization"] = f"Bearer {token}"
