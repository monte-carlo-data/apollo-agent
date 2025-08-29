import hashlib
from typing import Dict, Optional

import psycopg2
from psycopg2 import DatabaseError
from psycopg2.errors import QueryCanceled, InsufficientPrivilege  # noqa

from apollo.integrations.db.base_db_proxy_client import (
    BaseDbProxyClient,
    logger,
    SslOptions,
)

_ATTR_CONNECT_ARGS = "connect_args"


class PostgresProxyClient(BaseDbProxyClient):
    """
    Proxy client for Postgres. Credentials are expected to be supplied under "connect_args" and
    will be passed directly to `psycopg2.connect`, so only attributes supported as parameters by
    `psycopg2.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], client_type: str = "postgres", **kwargs):  # type: ignore
        super().__init__(connection_type=client_type)
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"{client_type.capitalize()} agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        connect_args = {
            **credentials[_ATTR_CONNECT_ARGS],
        }
        ssl_options = SslOptions(**(credentials.get("ssl_options", {})))
        if ssl_options.ca_data:
            connect_args["sslmode"] = "verify-full"
            # Pyscopg2 only supports providing a path to the CA bundle. Write the
            # CA data to a temp file and provide that path in the connection args.
            # Use the hashed host in the temp file name to distinguish between
            # connections if there are multiple using SSL through this agent.
            hashed_host = hashlib.sha256(
                connect_args.get("host", "").encode()
            ).hexdigest()[:12]
            connect_args["sslrootcert"] = ssl_options.write_ca_data_to_temp_file(
                f"/tmp/{hashed_host}_ca_bundle.crt",
                upsert=True,
            )

        # we were having tcp keep alive issues in Azure, so we're forcing it now unless configured from the dc
        # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-KEEPALIVES
        if "keepalives" not in connect_args:
            connect_args["keepalives"] = 1  # enables tcp keep alive messages.
            connect_args["keepalives_idle"] = (
                30  # start sending keep-alive packets after 30 seconds of inactivity.
            )
            connect_args["keepalives_interval"] = (
                10  # re-send keep-alive messages not acknowledged after 10 secs.
            )
            connect_args["keepalives_count"] = (
                5  # 5 keep-alive messages lost before considering connection lost.
            )

        self._connection = psycopg2.connect(**connect_args)
        self._client_type = client_type
        logger.info(f"Opened connection to {client_type}")

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
