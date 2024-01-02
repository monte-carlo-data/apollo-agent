from typing import Dict, Optional

import psycopg2
from psycopg2 import DatabaseError
from psycopg2.errors import QueryCanceled, InsufficientPrivilege  # noqa

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


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
        if "keepalives" not in connect_args:
            connect_args["keepalives"] = 1  # enables tcp keep alive messages.
            connect_args[
                "keepalives_idle"
            ] = 30  # start sending keep-alive packets after 30 seconds of inactivity.
            connect_args[
                "keepalives_interval"
            ] = 10  # re-send keep-alive messages not acknowledged after 10 secs.
            connect_args[
                "keepalives_count"
            ] = 5  # 5 keep-alive messages lost before considering connection lost.

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
