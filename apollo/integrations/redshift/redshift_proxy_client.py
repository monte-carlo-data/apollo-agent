from typing import Dict, Optional

import psycopg2
from psycopg2 import DatabaseError
from psycopg2.errors import QueryCanceled, InsufficientPrivilege  # noqa

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class RedshiftProxyClient(BaseDbProxyClient):
    """
    Proxy client for Redshift.
    Credentials are expected to be supplied under "connect_args" and will be passed directly to `psycopg2.connect`, so
    only attributes supported as parameters by `psycopg2.connect` should be passed.
    If "autocommit" is present in credentials it will be set in _connection.autocommit.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs):  # type: ignore
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Redshift agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        self._connection = psycopg2.connect(
            **credentials[_ATTR_CONNECT_ARGS],
        )
        if credentials.get("autocommit", False):
            self._connection.autocommit = True

    @property
    def wrapped_client(self):
        return self._connection

    def get_error_type(self, error: Exception) -> Optional[str]:
        """
        Convert PG errors QueryCanceled, InsufficientPrivilege and DatabaseError to error types
        that can be converted back to PG errors client side.
        """
        if isinstance(error, QueryCanceled):
            return "QueryCanceled"
        elif isinstance(error, InsufficientPrivilege):
            return "InsufficientPrivilege"
        elif isinstance(error, DatabaseError):
            return "DatabaseError"
        return super().get_error_type(error)
