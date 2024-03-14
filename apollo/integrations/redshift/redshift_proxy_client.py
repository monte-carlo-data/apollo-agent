import logging
from typing import Dict, Optional
from psycopg2.extensions import register_type, BYTES, BYTESARRAY
from apollo.integrations.db.postgres_proxy_client import PostgresProxyClient

_ATTR_CONNECT_ARGS = "connect_args"

logger = logging.getLogger(__name__)


class RedshiftProxyClient(PostgresProxyClient):
    """
    Proxy client for Redshift.
    Credentials are expected to be supplied under "connect_args" and will be passed directly to `psycopg2.connect`, so
    only attributes supported as parameters by `psycopg2.connect` should be passed.
    If "autocommit" is present in credentials it will be set in _connection.autocommit.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs):  # type: ignore
        PostgresProxyClient.__init__(
            self, credentials=credentials, client_type="redshift"
        )
        if credentials and credentials.get("autocommit", False):
            self._connection.autocommit = True

    def disable_decoding_in_driver(self):
        """
        Used when Redshift tables have mixed or unusual encodings. The effect is that string
        columns are returned as bytes, so decoding needs to happen in custom code instead of psycopg2.
        """
        logger.info("redshift_driver_decoding disabled")

        register_type(BYTES, self._connection)
        register_type(BYTESARRAY, self._connection)
