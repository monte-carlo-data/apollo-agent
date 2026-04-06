from typing import (
    Any,
    Optional,
)

import pyodbc

from apollo.integrations.db.tsql_base_db_proxy_client import (
    TSqlBaseDbProxyClient,
    odbc_string_from_dict,
)

_ATTR_CONNECT_ARGS = "connect_args"


class SqlServerProxyClient(TSqlBaseDbProxyClient):
    """
    Proxy client for SQL Server Client. Credentials are expected to be supplied under "connect_args"
    and will be passed directly to `pyodbc.connect`. 'pyodbc' accepts a connection string contained the connection details,
    the expectation from the DC is that _ATTR_CONNECT_ARGS will be a string.
    """

    _DEFAULT_LOGIN_TIMEOUT_IN_SECONDS = 15
    _DEFAULT_QUERY_TIMEOUT_IN_SECONDS = 60 * 14  # 14 minutes

    def __init__(self, credentials: Optional[dict], **kwargs: Any):
        super().__init__(connection_type="sql-server")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"SQL Server agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        connect_args = credentials[_ATTR_CONNECT_ARGS]
        connection_string = (
            odbc_string_from_dict(connect_args)
            if isinstance(connect_args, dict)
            else connect_args
        )
        self._connection = pyodbc.connect(
            connection_string,
            # Set timeout for establishing connection to db
            timeout=credentials.get(
                "login_timeout", self._DEFAULT_LOGIN_TIMEOUT_IN_SECONDS
            ),
        )  # type: ignore

        # Add output converter to handle datetimeoffset data types that are not supported by pyodbc
        self._connection.add_output_converter(
            self._DATETIMEOFFSET_SQL_TYPE_CODE, self._handle_datetimeoffset
        )

        # Set timeout for any query executed through this connection
        self._connection.timeout = credentials.get(
            "query_timeout_in_seconds", self._DEFAULT_QUERY_TIMEOUT_IN_SECONDS
        )

    @property
    def wrapped_client(self):
        return self._connection
