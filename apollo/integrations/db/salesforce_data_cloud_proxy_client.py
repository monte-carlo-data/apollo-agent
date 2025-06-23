from salesforcecdpconnector.connection import SalesforceCDPConnection
from salesforcecdpconnector.genie_table import GenieTable, Field

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient


class SalesforceDataCloudCredentials:
    def __init__(
        self,
        host: str,
        client_id: str,
        client_secret: str,
        core_token: str,
        refresh_token: str,
    ):
        self.host = host
        self.client_id = client_id
        self.client_secret = client_secret
        self.core_token = core_token
        self.refresh_token = refresh_token


class SalesforceDataCloudProxyClient(BaseDbProxyClient):
    def __init__(self, credentials: SalesforceDataCloudCredentials):
        super().__init__(connection_type="salesforce-data-cloud")
        self._connection = SalesforceCDPConnection(
            f"https://{credentials.host}",
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            core_token=credentials.core_token,
            refresh_token=credentials.refresh_token,
        )

    @property
    def wrapped_client(self):
        return self._connection

    def close(self):
        self._connection.close()

    def list_tables(self) -> list[dict]:
        tables: list[GenieTable] = self._connection.list_tables()
        return [self._serialize_table(table) for table in tables]

    def _serialize_table(self, table: GenieTable) -> dict:
        fields: list[Field] = table.fields
        return {
            "name": table.name,
            "display_name": table.display_name,
            "category": table.category,
            "fields": [
                {
                    "name": field.name,
                    "displayName": field.display_name,
                    "type": field.type,
                }
                for field in fields
            ],
        }
