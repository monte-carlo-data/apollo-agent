from typing import (
    Any,
    Dict,
    Iterable,
    Optional,
)

import pymssql

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class SqlServerProxyClientCursor:
    def __init__(self, wrapped_cursor: Any):
        self._wrapped_cursor = wrapped_cursor

    @property
    def description(self) -> Any:
        return self._wrapped_cursor.description

    def execute(self, query: str, params: Optional[Iterable] = None, **kwargs: Any):
        self._wrapped_cursor.execute(query, tuple(params) if params else None, **kwargs)

    def fetchall(self) -> Any:
        return self._wrapped_cursor.fetchall()

    def fetchmany(self, size: int) -> Any:
        return self._wrapped_cursor.fetchmany(size)

    @property
    def rowcount(self) -> Any:
        return self._wrapped_cursor.rowcount


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

    def cursor(self) -> Any:
        return SqlServerProxyClientCursor(self.wrapped_client.cursor())
