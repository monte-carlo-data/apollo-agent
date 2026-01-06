import hashlib
from typing import Any
import logging
import os
import tempfile

import ibm_db
import ibm_db_dbi

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient, SslOptions

_ATTR_CONNECT_ARGS = "connect_args"

logger = logging.getLogger(__name__)


class Db2ProxyClient(BaseDbProxyClient):
    """
    Proxy client for IBM DB2. Credentials are expected to be supplied under "connect_args" as
    a dictionary with connection parameters that will be used to build the DB2 connection string.

    Common parameters: DATABASE, HOSTNAME, PORT, PROTOCOL, UID, PWD, etc.
    """

    def __init__(self, credentials: dict | None, **kwargs: Any):
        super().__init__(connection_type="db2")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"DB2 agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        connect_args = {**credentials[_ATTR_CONNECT_ARGS]}

        # Handle SSL options for DB2 connections
        ssl_options = SslOptions(**(credentials.get("ssl_options") or {}))

        if ssl_options.ca_data and not ssl_options.disabled:
            # DB2 requires SSLServerCertificate to point to a certificate file
            # Create a temporary file for the CA certificate
            host_hash = hashlib.sha256(
                connect_args.get("hostname", "temp").encode()
            ).hexdigest()[:12]
            cert_file = f"/tmp/{host_hash}_db2_ca.pem"
            ssl_options.write_ca_data_to_temp_file(cert_file, upsert=True)

            connect_args["Security"] = "SSL"
            connect_args["SSLServerCertificate"] = cert_file

            logger.info("DB2 SSL configured")

        # Build connection string from dictionary parameters
        connection_string = ";".join(
            "=".join([key, str(value)]) for key, value in connect_args.items()
        )

        try:
            # Connect using ibm_db and create DBI connection wrapper
            # The DBI connection will manage the native connection lifecycle
            native_connection = ibm_db.connect(connection_string, "", "")
            self._connection = ibm_db_dbi.Connection(native_connection)
            logger.info("Opened connection to DB2")
        except Exception as e:
            logger.error(f"Failed to connect to DB2: {e}")
            raise

    @property
    def wrapped_client(self):
        return self._connection

    def get_error_type(self, error: Exception) -> str | None:
        """
        Convert DB2 specific errors to error types that can be handled client side.
        """
        # ibm_db_dbi uses standard DB-API exceptions, but we can add DB2-specific handling here
        error_str = str(error).lower()

        if "sql0204n" in error_str or "undefined name" in error_str:
            return "ObjectNotFound"
        elif "sql0551n" in error_str or "not authorized" in error_str:
            return "InsufficientPrivilege"
        elif "sql0911n" in error_str or "deadlock" in error_str:
            return "DeadlockError"
        elif "sql0952n" in error_str or "processing was cancelled" in error_str:
            return "QueryCanceled"

        return super().get_error_type(error)
