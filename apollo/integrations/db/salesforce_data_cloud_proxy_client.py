from dataclasses import dataclass
from typing import Any

from salesforcecdpconnector.connection import SalesforceCDPConnection
from salesforcecdpconnector.genie_table import GenieTable, Field

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient


class SalesforceDataCloudConnection(SalesforceCDPConnection):
    def __init__(
        self,
        login_url: str,
        client_id: str,
        client_secret: str,
        core_token: str | None = None,
        refresh_token: str | None = None,
    ):
        """
        SalesforceCDPConnection is designed to use a refresh token.
        After it exchanges the given core_token for a Data Cloud API token,
        it then revokes the core_token with the assumption that it can get a new one using the refresh token.

        In order to support client credentials, which doesn't involve a refresh token, we need to do a bit of a hack:
        1. Pass a fake refresh token
        2. Prevent the core token from being revoked
        """

        refresh_token = (
            None if refresh_token == "required_but_not_used" else refresh_token
        )  # Todo: remove this once data collectors are upgraded

        super().__init__(
            login_url,
            client_id=client_id,
            client_secret=client_secret,
            core_token=core_token,
            refresh_token=(refresh_token or "required_but_not_used"),
        )

        if refresh_token is None:

            def noop(*args: Any, **kwargs: Any):
                pass

            if (
                hasattr(self, "authentication_helper")
                and self.authentication_helper
                and hasattr(self.authentication_helper, "_revoke_core_token")
            ):
                # Prevent core token from being revoked.
                self.authentication_helper._revoke_core_token = noop
            else:
                raise Exception(
                    "salesforce-cdp-connector library has changed. Cannot override _revoke_core_token()"
                )


@dataclass
class SalesforceDataCloudCredentials:
    domain: str
    client_id: str
    client_secret: str
    core_token: str | None
    refresh_token: str | None


class SalesforceDataCloudProxyClient(BaseDbProxyClient):
    def __init__(self, credentials: SalesforceDataCloudCredentials):
        super().__init__(connection_type="salesforce-data-cloud")
        self._connection = SalesforceDataCloudConnection(
            f"https://{credentials.domain}",
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
