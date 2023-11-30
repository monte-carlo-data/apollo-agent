from typing import Optional, Dict, Any

import pyodbc

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class AzureDedicatedSqlPoolProxyClient(BaseDbProxyClient):
    """
    Proxy client for Azure Dedicated SQL Pool Client. Credentials are expected to be supplied under "connect_args"
    and will be passed directly to `pyodbc.connect`, so only attributes supported as parameters
    by `pyodbc.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Azure Dedicated SQL Pool agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        self._connection = pyodbc.connect(**credentials[_ATTR_CONNECT_ARGS])  # type: ignore

    @property
    def wrapped_client(self):
        return self._connection
