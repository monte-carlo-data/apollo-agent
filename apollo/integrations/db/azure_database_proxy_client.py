from typing import Optional, Any

import pyodbc

from apollo.integrations.db.tsql_base_db_proxy_client import TSqlBaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class AzureDatabaseProxyClient(TSqlBaseDbProxyClient):
    """
    Proxy client for Azure "database" Client which is used by the Azure Dedicated SQL Pool and Azure SQL Database connections.
    Credentials are expected to be supplied under "connect_args" and will be passed directly to `pyodbc.connect`.
    'pyodbc` accepts a connection string with the connection details, so "connect_args" will be a string.
    """

    _DEFAULT_LOGIN_TIMEOUT_IN_SECONDS = 15
    _DEFAULT_QUERY_TIMEOUT_IN_SECONDS = 60 * 14  # 14 minutes

    def __init__(self, credentials: Optional[dict], **kwargs: Any):
        super().__init__(connection_type="azure-database")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Azure database agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        self._connection = pyodbc.connect(
            credentials[_ATTR_CONNECT_ARGS],
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
