from typing import Optional, Dict, Any, Union

import pyodbc

from apollo.integrations.db.tsql_base_db_proxy_client import TSqlBaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


def _odbc_escape(value: str) -> str:
    """Escape an ODBC connection string value by wrapping in braces if it contains special chars.

    ODBC connection string values containing ``;``, ``{``, ``}``, or ``=`` must be wrapped
    in curly braces to prevent them from being interpreted as key-value delimiters.
    Any literal ``}`` inside the value is doubled (``}}``) per the ODBC spec.

    Values that are already wrapped in a matching ``{...}`` pair (e.g. driver names like
    ``{ODBC Driver 18 for SQL Server}``) are left unchanged — they are already correctly
    quoted for ODBC.
    """
    if value.startswith("{") and value.endswith("}"):
        # Already brace-wrapped (e.g. driver names) — leave as-is.
        return value
    if any(c in value for c in (";", "{", "}", "=")):
        return "{" + value.replace("}", "}}") + "}"
    return value


class MsFabricProxyClient(TSqlBaseDbProxyClient):
    """Proxy client for Microsoft Fabric SQL Warehouse connections via ODBC.

    This client connects to a Microsoft Fabric SQL Warehouse endpoint using pyodbc.
    The ``connect_args`` credential field accepts either:

    - A ``str``: passed directly to pyodbc as the ODBC connection string.
    - A ``dict``: serialized to ODBC connection string format by joining each
      key/value pair as ``"key=value"`` separated by semicolons. Values that contain
      special ODBC characters (``;``, ``{``, ``}``, ``=``) are automatically wrapped
      in curly braces per the ODBC spec, e.g.
      ``{"Driver": "{ODBC Driver 18 for SQL Server}", "Server": "..."}`` becomes
      ``"Driver={ODBC Driver 18 for SQL Server};Server=..."``.

    Optional credential keys:
    - ``login_timeout``: seconds to wait when establishing a connection (default 15).
    - ``query_timeout_in_seconds``: seconds to wait for a query result (default 840).
    """

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
            connection_string = ";".join(
                f"{k}={_odbc_escape(str(v))}" for k, v in connect_args.items()
            )
        elif isinstance(connect_args, str):
            connection_string = connect_args
        else:
            raise ValueError(
                f"{_ATTR_CONNECT_ARGS} must be a dict or str, "
                f"got {type(connect_args).__name__}"
            )
        self._connection = pyodbc.connect(
            connection_string,
            timeout=credentials.get(
                "login_timeout", self._DEFAULT_LOGIN_TIMEOUT_IN_SECONDS
            ),
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

