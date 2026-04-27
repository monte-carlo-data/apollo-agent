from typing import Dict, Optional

from databricks import sql

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class DatabricksSqlWarehouseProxyClient(BaseDbProxyClient):
    """
    Proxy client for Databricks SQL Warehouse Client.
    Credentials are expected to be supplied under "connect_args" and will be passed directly to
    `sql.connect`. The CTP pipeline handles auth (PAT access_token or OAuth credentials_provider
    callable) and URL normalization before the proxy is constructed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Dict):
        super().__init__(connection_type="databricks-sql-warehouse")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Databricks agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        self._connection = sql.connect(**credentials[_ATTR_CONNECT_ARGS])

    @property
    def wrapped_client(self):
        return self._connection
