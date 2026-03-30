import struct
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Union

import pyodbc

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class MsFabricProxyClient(BaseDbProxyClient):
    """Proxy client for Microsoft Fabric SQL Warehouse connections via ODBC.

    This client connects to a Microsoft Fabric SQL Warehouse endpoint using pyodbc.
    The ``connect_args`` credential field accepts either:

    - A ``str``: passed directly to pyodbc as the ODBC connection string.
    - A ``dict``: serialized to ODBC connection string format by joining each
      key/value pair as ``"key=value"`` separated by semicolons, e.g.
      ``{"Driver": "{ODBC Driver 18 for SQL Server}", "Server": "..."}`` becomes
      ``"Driver={ODBC Driver 18 for SQL Server};Server=..."``.

    Optional credential keys:
    - ``login_timeout``: seconds to wait when establishing a connection (default 15).
    - ``query_timeout_in_seconds``: seconds to wait for a query result (default 840).
    """

    _DATETIMEOFFSET_SQL_TYPE_CODE = -155
    _DEFAULT_LOGIN_TIMEOUT_IN_SECONDS = 15
    _DEFAULT_QUERY_TIMEOUT_IN_SECONDS = 60 * 14  # 14 minutes

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        super().__init__(connection_type="microsoft-fabric")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Microsoft Fabric agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        connect_args: Union[str, dict] = credentials[_ATTR_CONNECT_ARGS]
        if isinstance(connect_args, dict):
            connection_string = ";".join(f"{k}={v}" for k, v in connect_args.items())
        elif isinstance(connect_args, str):
            connection_string = connect_args
        else:
            raise ValueError(
                f"{_ATTR_CONNECT_ARGS} must be a dict or str, "
                f"got {type(connect_args).__name__}"
            )
        self._connection = pyodbc.connect(
            connection_string,
            timeout=credentials.get("login_timeout", self._DEFAULT_LOGIN_TIMEOUT_IN_SECONDS),
        )
        self._connection.add_output_converter(
            self._DATETIMEOFFSET_SQL_TYPE_CODE, self._handle_datetimeoffset
        )
        self._connection.timeout = credentials.get(
            "query_timeout_in_seconds", self._DEFAULT_QUERY_TIMEOUT_IN_SECONDS
        )

    @property
    def wrapped_client(self):
        return self._connection

    @classmethod
    def _process_description(cls, col: List) -> List:
        return [col[0], col[1].__name__, col[2], col[3], col[4], col[5], col[6]]

    @staticmethod
    def _handle_datetimeoffset(dto_value: bytes) -> datetime:
        tup = struct.unpack("<6hI2h", dto_value)
        return datetime(
            tup[0], tup[1], tup[2], tup[3], tup[4], tup[5],
            tup[6] // 1000,
            timezone(timedelta(hours=tup[7], minutes=tup[8])),
        )
