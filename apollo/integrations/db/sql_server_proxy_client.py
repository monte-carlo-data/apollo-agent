from typing import (
    Any,
    Dict,
    Optional,
)

import pymssql

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class SqlServerProxyClient(BaseDbProxyClient):
    """
    Proxy client for SQL Server Client. Credentials are expected to be supplied under "connect_args"
    and will be passed directly to `pymssql.connect`, so only attributes supported as parameters
    by `pymssql.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"SQL Server agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        self._connection = pymssql.connect(**credentials[_ATTR_CONNECT_ARGS])  # type: ignore

    @property
    def wrapped_client(self):
        return self._connection