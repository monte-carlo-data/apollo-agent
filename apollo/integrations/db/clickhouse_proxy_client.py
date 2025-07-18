from typing import Any, Dict, Optional

import clickhouse_connect.dbapi

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class ClickHouseProxyClient(BaseDbProxyClient):
    """
    Proxy client for ClickHouse.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        super().__init__(connection_type="clickhouse")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Clickhouse agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        connect_args = credentials[_ATTR_CONNECT_ARGS]
        self._connection = clickhouse_connect.dbapi.connect(**connect_args)

    @property
    def wrapped_client(self):
        return self._connection
