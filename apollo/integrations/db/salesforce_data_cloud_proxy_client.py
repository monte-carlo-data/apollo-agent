import logging
from dataclasses import dataclass
from typing import Any

from salesforcecdpconnector.connection import SalesforceCDPConnection
from salesforcecdpconnector.genie_table import GenieTable, Field

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

logger = logging.getLogger(__name__)


class SalesforceDataCloudConnection(SalesforceCDPConnection):
    def __init__(
        self,
        login_url: str,
        client_id: str,
        client_secret: str,
        core_token: str | None = None,
        refresh_token: str | None = None,
        dataspace: str | None = None,
    ):
        """
        SalesforceCDPConnection is designed to use a refresh token.
        After it exchanges the given core_token for a Data Cloud API token,
        it then revokes the core_token with the assumption that it can get a new one using the refresh token.

        In order to support client credentials, which doesn't involve a refresh token, we need to:
        1. Pass a fake refresh token (so the library enters the exchange path)
        2. Prevent the core token from being revoked (so it can be reused across dataspaces)
        3. Prevent the _renew_token fallback (so exchange failures raise a clear error instead of
           "Token Renewal failed with code 400" from attempting to renew with the fake token)

        "required_but_not_used" is normalized to None for backwards compatibility with
        old data-collectors that sent it before this change.
        """

        # Normalize legacy value sent by old data-collectors.
        if refresh_token == "required_but_not_used":
            refresh_token = None

        super().__init__(
            login_url,
            client_id=client_id,
            client_secret=client_secret,
            core_token=core_token,
            refresh_token=(refresh_token or "required_but_not_used"),
            dataspace=dataspace,
        )

        if refresh_token is None:

            def noop(*args: Any, **kwargs: Any) -> None:
                pass

            def raise_on_renewal(*args: Any, **kwargs: Any) -> tuple[Any, Any]:
                raise Exception(
                    "Token exchange failed. The access token may have expired or the dataspace may not exist."
                )

            if (
                hasattr(self, "authentication_helper")
                and self.authentication_helper
                and hasattr(self.authentication_helper, "_revoke_core_token")
            ):
                # Prevent core token from being revoked.
                self.authentication_helper._revoke_core_token = noop
                self.authentication_helper._renew_token = raise_on_renewal
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
    # Accepted for backwards compatibility; iteration over dataspaces is handled
    # by the data-collector, which calls list_tables(dataspace=X) once per dataspace.
    dataspaces: list[str] | None = None


class SalesforceDataCloudProxyClient(BaseDbProxyClient):
    def __init__(self, credentials: SalesforceDataCloudCredentials):
        super().__init__(connection_type="salesforce-data-cloud")
        self._credentials = credentials
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

    def list_tables(self, dataspace: str | None = None) -> list[dict]:
        if dataspace is not None:
            logger.info(
                "Salesforce Data Cloud: fetching tables for dataspace",
                extra={"dataspace": dataspace},
            )
            # Create a temporary connection scoped to this dataspace.
            # The dataspace param is passed to /services/a360/token exchange,
            # which scopes the resulting Data Cloud token to that dataspace.
            conn = SalesforceDataCloudConnection(
                f"https://{self._credentials.domain}",
                client_id=self._credentials.client_id,
                client_secret=self._credentials.client_secret,
                core_token=self._credentials.core_token,
                refresh_token=self._credentials.refresh_token,
                dataspace=dataspace,
            )
            try:
                tables: list[GenieTable] = conn.list_tables()
            finally:
                conn.close()
            logger.info(
                "Salesforce Data Cloud: fetched tables for dataspace",
                extra={"dataspace": dataspace, "table_count": len(tables)},
            )
        else:
            logger.info("Salesforce Data Cloud: fetching tables (unscoped)")
            tables = self._connection.list_tables()
            logger.info(
                "Salesforce Data Cloud: fetched tables (unscoped)",
                extra={"table_count": len(tables)},
            )
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
