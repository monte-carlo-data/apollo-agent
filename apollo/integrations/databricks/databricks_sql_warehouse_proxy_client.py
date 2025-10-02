from typing import Dict, Optional, Callable

from databricks import sql
from databricks.sdk.core import oauth_service_principal, azure_service_principal, Config

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"
_ATTR_CREDENTIALS_PROVIDER = "credentials_provider"

SERVER_HOSTNAME = "server_hostname"
CLIENT_ID_KEY = "databricks_client_id"
CLIENT_SECRET_KEY = "databricks_client_secret"
AZURE_TENANT_ID = "azure_tenant_id"
AZURE_WORKSPACE_RESOURCE_ID = "azure_workspace_resource_id"


class DatabricksSqlWarehouseProxyClient(BaseDbProxyClient):
    """
    Proxy client for Databricks SQL Warehouse Client.
    Credentials are expected to be supplied under "connect_args" and will be passed directly to `sql.connect`, so
    only attributes supported as parameters by `sql.connect` should be passed.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Dict):
        super().__init__(connection_type="databricks-sql-warehouse")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Databricks agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        if self._credentials_use_oauth(credentials[_ATTR_CONNECT_ARGS]):
            credentials[_ATTR_CONNECT_ARGS][_ATTR_CREDENTIALS_PROVIDER] = (
                self._oauth_credentials_provider(credentials[_ATTR_CONNECT_ARGS])
            )

        self._connection = sql.connect(**credentials[_ATTR_CONNECT_ARGS])

    def _credentials_use_oauth(self, connect_args: Dict) -> bool:
        return CLIENT_ID_KEY in connect_args and CLIENT_SECRET_KEY in connect_args

    def _oauth_credentials_provider(self, connect_args: Dict) -> Callable:
        """Factory for OAuth credentials provider (Azure or Databricks-managed)."""
        host = connect_args.get(SERVER_HOSTNAME)

        is_azure_managed = all(
            connect_args.get(k) for k in (AZURE_TENANT_ID, AZURE_WORKSPACE_RESOURCE_ID)
        )

        if is_azure_managed:
            config = Config(
                host=host,
                azure_client_id=connect_args.get(CLIENT_ID_KEY),
                azure_client_secret=connect_args.get(CLIENT_SECRET_KEY),
                azure_tenant_id=connect_args.get(AZURE_TENANT_ID),
                azure_workspace_resource_id=connect_args.get(
                    AZURE_WORKSPACE_RESOURCE_ID
                ),
            )
            provider = azure_service_principal
        else:
            config = Config(
                host=host,
                client_id=connect_args.get(CLIENT_ID_KEY),
                client_secret=connect_args.get(CLIENT_SECRET_KEY),
            )
            provider = oauth_service_principal

        return lambda: provider(config)

    @property
    def wrapped_client(self):
        return self._connection
