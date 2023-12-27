from typing import Dict, Optional

import psycopg2
from psycopg2 import DatabaseError
from psycopg2.errors import QueryCanceled, InsufficientPrivilege  # noqa

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"
_KEEP_ALIVE_SETTING = "keepalives"


class PostgresProxyClient(BaseDbProxyClient):
    """
    Proxy client for Postgres. Credentials are expected to be supplied under "connect_args" and
    will be passed directly to `psycopg2.connect`, so only attributes supported as parameters by
    `psycopg2.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], client_type: str = "postgres", **kwargs):  # type: ignore
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"{client_type.capitalize()} agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        connect_args = {
            **credentials[_ATTR_CONNECT_ARGS],
        }
        # we were having tcp keep alive issues in Azure, so we're forcing it now unless configured from the dc
        # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-KEEPALIVES
        if _KEEP_ALIVE_SETTING not in connect_args:
            connect_args[_KEEP_ALIVE_SETTING] = 1
            connect_args[f"{_KEEP_ALIVE_SETTING}_idle"] = 30
            connect_args[f"{_KEEP_ALIVE_SETTING}_interval"] = 10
            connect_args[f"{_KEEP_ALIVE_SETTING}_count"] = 5

        self._connection = psycopg2.connect(**connect_args)

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
