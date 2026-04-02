import logging
from dataclasses import dataclass
from typing import Any, NoReturn

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
        # Normalize legacy value sent by old data-collectors.
        if refresh_token == "required_but_not_used":
            refresh_token = None

        if core_token is not None:
            # Old DC path: exchange the externally-provided core_token.
            # Pass a fake refresh_token so the library enters the exchange path.
            # Override _revoke_core_token so the same token can be reused across
            # multiple per-dataspace connections without being revoked after the first.
            # Override _renew_token to raise a clear error instead of the misleading
            # "Token Renewal failed with code 400" if the exchange itself fails.
            super().__init__(
                login_url,
                client_id=client_id,
                client_secret=client_secret,
                core_token=core_token,
                refresh_token="required_but_not_used",
                dataspace=dataspace,
            )

            def noop(*args: Any, **kwargs: Any) -> None:
                pass

            def raise_on_renewal(*args: Any, **kwargs: Any) -> NoReturn:
                raise Exception(
                    "Token exchange failed. The access token may have expired or the dataspace may not exist."
                )

            if (
                hasattr(self, "authentication_helper")
                and self.authentication_helper
                and hasattr(self.authentication_helper, "_revoke_core_token")
                and hasattr(self.authentication_helper, "_renew_token")
            ):
                self.authentication_helper._revoke_core_token = noop
                self.authentication_helper._renew_token = raise_on_renewal
            else:
                raise Exception(
                    "salesforce-cdp-connector library has changed. "
                    "Cannot override _revoke_core_token() and _renew_token()."
                )
        else:
            # New DC path: let the library handle OAuth + exchange via client credentials.
            # _token_by_client_creds_flow fetches a core token then exchanges it for
            # a scoped Data Cloud token. No overrides needed.
            super().__init__(
                login_url,
                client_id=client_id,
                client_secret=client_secret,
                dataspace=dataspace,
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
                f"Salesforce Data Cloud: fetching tables for dataspace '{dataspace}'",
                extra={"dataspace": dataspace},
            )
            # Create a temporary connection scoped to this dataspace.
            #
            # IMPORTANT:
            # For dataspace-scoped collection we intentionally do NOT reuse the pre-fetched
            # core_token from data-collector. Reusing the same core token across multiple
            # dataspaces can result in ambiguous scoping behavior on the Salesforce side.
            # Using the clean client-credentials flow here obtains a fresh core token and
            # performs a dataspace-scoped a360 exchange for each dataspace attempt.
            conn = SalesforceDataCloudConnection(
                f"https://{self._credentials.domain}",
                client_id=self._credentials.client_id,
                client_secret=self._credentials.client_secret,
                core_token=None,
                refresh_token=None,
                dataspace=dataspace,
            )
            try:
                tables: list[GenieTable] = conn.list_tables()
            except Exception as e:
                raise RuntimeError(
                    f"Token exchange failed for dataspace '{dataspace}': "
                    "verify the dataspace exists and credentials are valid"
                ) from e
            finally:
                conn.close()
            logger.info(
                f"Salesforce Data Cloud: fetched tables for dataspace '{dataspace}'",
                extra={"dataspace": dataspace, "table_count": len(tables)},
            )
        else:
            logger.info("Salesforce Data Cloud: fetching tables (unscoped)")
            try:
                tables = self._connection.list_tables()
            except Exception as e:
                raise RuntimeError(
                    "Token exchange failed: verify credentials are valid"
                ) from e
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
