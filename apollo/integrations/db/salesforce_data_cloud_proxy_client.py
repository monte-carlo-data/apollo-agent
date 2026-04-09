import logging
from dataclasses import dataclass
from typing import Any, NoReturn

import requests
from salesforcecdpconnector.connection import SalesforceCDPConnection
from salesforcecdpconnector.exceptions import Error as SalesforceCDPError
from salesforcecdpconnector.genie_table import GenieTable, Field

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

logger = logging.getLogger(__name__)


class _CapturingSession(requests.Session):
    """
    Wraps the library's requests.Session to capture the Salesforce a360/token response
    body regardless of status code.

    The salesforcecdpconnector library raises Error('CDP token retrieval failed with
    code N') on non-200 and discards the body. On 200 responses with unexpected payloads
    (e.g. Salesforce returning 200 with an error body for unknown dataspaces) the library
    raises KeyError. In both cases the body is discarded before we can see it.

    Capturing on all a360/token responses means we always have it available to include
    in the RuntimeError that propagates back to the data-collector and into Datadog logs.
    """

    def __init__(self) -> None:
        super().__init__()
        self.last_exchange_body: str | None = None
        self.last_exchange_status: int | None = None

    def post(self, url: str, **kwargs: Any):  # type: ignore[override]
        response = super().post(url, **kwargs)
        if "a360/token" in url:
            self.last_exchange_status = response.status_code
            try:
                self.last_exchange_body = str(response.json())
            except Exception:
                self.last_exchange_body = response.text[:500]
        return response


def _classify_exchange_status(status: int | None) -> str:
    """Return a short error-type label for a Salesforce a360/token HTTP status code."""
    if status is None:
        return "unknown"
    if status == 429:
        return "rate_limited"
    if status in (401, 403):
        return "auth_failed"
    if status == 400:
        return "bad_request"
    if status >= 500:
        return "server_error"
    return "other"


import re as _re

_ACCESS_TOKEN_PATTERN = _re.compile(r"('access_token'\s*:\s*)'[^']*'")


def _redact_body(body: str | None) -> str | None:
    """
    Redact access_token values from a captured a360/token response body string
    before including it in error messages or log records.

    The body is stored as str(response.json()), so token values appear as
    Python string literals: 'access_token': 'eyJ...'. Redacting prevents
    accidental credential exposure when an unrelated error fires after a
    successful token exchange.
    """
    if body is None:
        return None
    return _ACCESS_TOKEN_PATTERN.sub(r"\1'[REDACTED]'", body)


def _attach_capturing_session(
    conn: "SalesforceDataCloudConnection",
) -> _CapturingSession | None:
    """
    Replace the requests.Session on the connection's authentication_helper with a
    _CapturingSession so that on failure the response body can be included in the
    RuntimeError propagated back to the data-collector (and visible in Datadog).

    Returns the capturing session so the caller can read last_exchange_body and
    last_exchange_status after a failed call, or None if the library internals
    have changed.
    """
    if not (
        hasattr(conn, "authentication_helper")
        and conn.authentication_helper
        and hasattr(conn.authentication_helper, "session")
    ):
        return None

    original = conn.authentication_helper.session
    capturing = _CapturingSession()
    capturing.headers.update(original.headers)
    capturing.cookies.update(original.cookies)
    conn.authentication_helper.session = capturing
    return capturing


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
    # Single dataspace scope for query execution (profiling, monitors, validation).
    # When set, the a360/token exchange is scoped to this dataspace so queries
    # against tables in non-default dataspaces succeed.
    dataspace: str | None = None


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
            dataspace=credentials.dataspace,
        )

    @property
    def wrapped_client(self):
        return self._connection

    def close(self):
        self._connection.close()

    def list_tables(self, dataspace: str | None = None) -> list[dict]:
        if dataspace is not None:
            logger.info(
                f"Salesforce Data Cloud: fetching tables for dataspace '{dataspace}' "
                f"(domain={self._credentials.domain}, "
                f"client_id={self._credentials.client_id[:8]}...)",
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
            capturing = _attach_capturing_session(conn)
            try:
                tables: list[GenieTable] = conn.list_tables()
            except SalesforceCDPError as e:
                body = _redact_body(capturing.last_exchange_body if capturing else None)
                status = capturing.last_exchange_status if capturing else None
                logger.warning(
                    "Salesforce Data Cloud: a360/token exchange failed for dataspace '%s'",
                    dataspace,
                    extra={
                        "dataspace": dataspace,
                        "exchange_status_code": status,
                        "exchange_error_type": _classify_exchange_status(status),
                        "exchange_error": str(e)[:500],
                        "exchange_response_body": body,
                    },
                )
                detail = f" (Salesforce response: {body})" if body else ""
                raise RuntimeError(
                    f"Token exchange failed for dataspace '{dataspace}': {e}{detail} — "
                    f"verify the dataspace name and that the connected app's Run-As user "
                    f"has permission for this dataspace"
                ) from e
            except KeyError as e:
                body = _redact_body(capturing.last_exchange_body if capturing else None)
                status = capturing.last_exchange_status if capturing else None
                logger.warning(
                    "Salesforce Data Cloud: a360/token exchange returned unexpected response for dataspace '%s'",
                    dataspace,
                    extra={
                        "dataspace": dataspace,
                        "exchange_status_code": status,
                        "exchange_error_type": "missing_access_token",
                        "missing_key": str(e),
                        "exchange_response_body": body,
                    },
                )
                detail = (
                    f" (HTTP {status}, Salesforce response: {body})" if body else ""
                )
                raise RuntimeError(
                    f"Token exchange failed for dataspace '{dataspace}': "
                    f"OAuth response missing key {e}{detail} — "
                    f"verify the dataspace exists and credentials are valid"
                ) from e
            finally:
                conn.close()
            logger.info(
                f"Salesforce Data Cloud: fetched tables for dataspace '{dataspace}'",
                extra={"dataspace": dataspace, "table_count": len(tables)},
            )
        else:
            logger.info(
                f"Salesforce Data Cloud: fetching tables (unscoped, "
                f"domain={self._credentials.domain})"
            )
            # If the base connection was created with a dataspace (for query execution),
            # use a fresh unscoped connection here so that list_tables(None) always
            # returns the default-dataspace view regardless of how this client was
            # instantiated.  This prevents a future caller from accidentally getting
            # dataspace-scoped results while believing the fetch is unscoped.
            if self._credentials.dataspace:
                unscoped_conn: SalesforceDataCloudConnection | None = (
                    SalesforceDataCloudConnection(
                        f"https://{self._credentials.domain}",
                        client_id=self._credentials.client_id,
                        client_secret=self._credentials.client_secret,
                        core_token=None,
                        refresh_token=None,
                        dataspace=None,
                    )
                )
            else:
                unscoped_conn = None
            conn_to_use = unscoped_conn or self._connection
            try:
                tables = conn_to_use.list_tables()
            except SalesforceCDPError as e:
                raise RuntimeError(
                    f"Token exchange failed: {e} — verify credentials are valid"
                ) from e
            except KeyError as e:
                raise RuntimeError(
                    f"Token exchange failed: OAuth response missing key {e} — "
                    f"verify credentials are valid"
                ) from e
            finally:
                if unscoped_conn is not None:
                    unscoped_conn.close()
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
