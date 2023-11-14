from typing import Dict, Optional

import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError
from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class SnowflakeProxyClient(BaseDbProxyClient):
    """
    Proxy client for Snowflake.
    Credentials are expected to be supplied under "connect_args" and will be passed directly to `psycopg2.connect`, so
    only attributes supported as parameters by `snowflake.connector.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs):  # type: ignore
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Snowflake agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        self._connection = snowflake.connector.connect(
            **credentials[_ATTR_CONNECT_ARGS],
        )

    @property
    def wrapped_client(self):
        return self._connection

    def get_error_type(self, error: Exception) -> Optional[str]:
        """
        Convert SF errors to error types that can be converted back to SF errors client side.
        """
        if isinstance(error, ProgrammingError):
            return "ProgrammingError"
        elif isinstance(error, DatabaseError):
            return "DatabaseError"
        return super().get_error_type(error)

    def get_error_extra_attributes(self, error: Exception) -> Optional[Dict]:
        """
        Return a dictionary with `errno` and `sqlstate` for SF Errors.
        """
        if isinstance(error, DatabaseError):  # ProgrammingError extends DatabaseError
            return {
                "errno": error.errno,
                "sqlstate": error.sqlstate,
            }
        return super().get_error_extra_attributes(error)
