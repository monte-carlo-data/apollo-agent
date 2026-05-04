from typing import Optional, Any

import pyodbc

from apollo.integrations.db.tsql_base_db_proxy_client import (
    TSqlBaseDbProxyClient,
    odbc_string_from_dict,
)

_ATTR_CONNECT_ARGS = "connect_args"


class MsFabricProxyClient(TSqlBaseDbProxyClient):
    """Proxy client for Microsoft Fabric SQL Warehouse connections via ODBC.

    Expects ``connect_args`` to be a dict of ODBC key-value pairs as produced by the
    CTP pipeline (e.g. ``DRIVER``, ``SERVER``, ``UID``).  The dict is serialized to an
    ODBC connection string via ``odbc_string_from_dict`` before calling ``pyodbc.connect``.

    Optional ``connect_args`` keys (popped before serialization):
    - ``login_timeout``: seconds to wait when establishing a connection (default 15).
    - ``query_timeout_in_seconds``: seconds to wait for a query result (default 840).
    """

    _DEFAULT_LOGIN_TIMEOUT_IN_SECONDS = 15
    _DEFAULT_QUERY_TIMEOUT_IN_SECONDS = 60 * 14  # 14 minutes

    def __init__(self, credentials: Optional[dict], **kwargs: Any):
        super().__init__(connection_type="microsoft-fabric")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Microsoft Fabric agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        connect_args = credentials[_ATTR_CONNECT_ARGS]
        if not isinstance(connect_args, dict):
            raise ValueError(
                f"{_ATTR_CONNECT_ARGS} must be a dict, "
                f"got {type(connect_args).__name__}"
            )
        # CTP path: timeout fields land in connect_args; pop before building ODBC string
        connect_args = dict(connect_args)
        login_timeout = connect_args.pop(
            "login_timeout", self._DEFAULT_LOGIN_TIMEOUT_IN_SECONDS
        )
        query_timeout = connect_args.pop(
            "query_timeout_in_seconds", self._DEFAULT_QUERY_TIMEOUT_IN_SECONDS
        )
        connection_string = odbc_string_from_dict(connect_args)

        self._connection = pyodbc.connect(
            connection_string,
            timeout=login_timeout,
        )
        self._connection.add_output_converter(
            self._DATETIMEOFFSET_SQL_TYPE_CODE, self._handle_datetimeoffset
        )
        self._connection.timeout = query_timeout

    @property
    def wrapped_client(self):
        return self._connection
