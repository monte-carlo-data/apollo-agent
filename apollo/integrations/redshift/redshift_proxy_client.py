from typing import Dict, Optional

from apollo.integrations.db.postgres_proxy_client import PostgresProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


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
