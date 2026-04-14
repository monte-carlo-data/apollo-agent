from typing import (
    Any,
    Dict,
    Optional,
)

from apollo.integrations.http.http_proxy_client import HttpProxyClient


class PowerBiProxyClient(HttpProxyClient):
    """
    PowerBI Proxy Client. Token is resolved by the CTP resolve_msal_token transform
    and passed through connect_args to HttpProxyClient.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):  # noqa
        if not credentials:
            raise ValueError("Credentials are required for PowerBI")
        super().__init__(credentials=credentials)
