from typing import Any, ClassVar, Dict, Optional

import clickhouse_connect.dbapi

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


# Cerberus schema for the customer-facing self-hosted credentials JSON. Lives
# on the proxy client because ClickHouse is not enrolled in CTP — the proxy
# passes connect_args verbatim to clickhouse_connect.dbapi.connect, so the
# customer fields ARE the driver kwargs.
CLICKHOUSE_CREDENTIALS_SCHEMA: Dict[str, Any] = {
    "connect_args": {
        "type": "dict",
        "required": True,
        "schema": {
            "host": {"type": "string", "required": True, "empty": False},
            "port": {"type": "integer", "required": True},
            "username": {"type": "string", "required": True, "empty": False},
            "password": {"type": "string", "required": True, "empty": False},
            "database": {"type": "string", "required": True, "empty": False},
        },
    },
}


class ClickHouseProxyClient(BaseDbProxyClient):
    """
    Proxy client for ClickHouse.
    """

    SELF_HOSTED_CREDENTIALS_SCHEMA: ClassVar[Dict[str, Any]] = (
        CLICKHOUSE_CREDENTIALS_SCHEMA
    )

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
