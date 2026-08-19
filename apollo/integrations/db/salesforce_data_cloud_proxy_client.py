import http.client
import logging
import re
import urllib.parse
from dataclasses import dataclass
from typing import Any, NoReturn

import requests
import urllib3.exceptions
from retry import retry
from retry.api import retry_call
from salesforcecdpconnector.connection import SalesforceCDPConnection
from salesforcecdpconnector.cursor import SalesforceCDPCursor
from salesforcecdpconnector.exceptions import Error as SalesforceCDPError
from salesforcecdpconnector.genie_table import GenieTable, Field

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient
from apollo.integrations.http.relative_path import validate_relative_rest_path

logger = logging.getLogger(__name__)

# Transient network errors that occur when a pooled HTTP connection to Salesforce's
# edge has been silently closed by the LB after idle, then the agent reuses it on
# the next request. Salesforce drops the TCP socket without sending an HTTP
# response, urllib3 raises ProtocolError(RemoteDisconnected(...)), and the call
# fails. Retrying on a fresh connection from the pool typically succeeds.
# See: hourly metric monitor failures with `AgentClientError. ('Connection
# aborted.', RemoteDisconnected('Remote end closed connection without response'))`.
_TRANSIENT_NETWORK_ERRORS: tuple[type[Exception], ...] = (
    urllib3.exceptions.ProtocolError,
    http.client.RemoteDisconnected,
    ConnectionResetError,
    ConnectionAbortedError,
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
)

# Retry config shared by `@retry` decorations and `retry_call(...)` invocations.
# tries includes the initial call; backoff multiplies delay each attempt up to
# max_delay. Logger is passed so retry-library warnings flow through this
# module's logger rather than retry.logging_logger.
_RETRY_KWARGS: dict[str, Any] = {
    "exceptions": _TRANSIENT_NETWORK_ERRORS,
    "tries": 3,
    "delay": 0.5,
    "max_delay": 4,
    "backoff": 2,
    "logger": logger,
}

# Salesforce core REST API version used by the SOQL dataspace discovery call
# (see ``SalesforceDataCloudProxyClient.list_dataspaces``). Validated against
# customer orgs returning DataSpaceApiName from the Dataspace SObject (YET-1256).
_SOQL_API_VERSION = "v62.0"
_LIST_DATASPACES_SOQL = "SELECT DataSpaceApiName FROM Dataspace"
# Bounds the SOQL dataspace discovery GET and the core-token mint POST. The mint
# bound is the ``30`` term in the data-collector's documented cross-repo deadman
# budget (``30 + timeout``, see ``ssot_get``) — changing it is not a local
# decision, it reflows through that budget in a separate repo. It is also the
# connect half of the ``/ssot`` GET's ``(connect, read)`` timeout tuple.
_DISCOVERY_REQUEST_TIMEOUT_SECONDS = 30

# Default read timeout for the ``/ssot`` GET when the caller sends nothing
# (YET-2440). Deliberately equal to the mint bound above today, but a SEPARATE
# knob: a pre-gate data-collector's page walk must stay bit-for-bit unchanged if
# the discovery bound is ever re-tuned, so the two must not be able to drift
# together by accident.
_SSOT_REQUEST_TIMEOUT_DEFAULT_SECONDS = 30

# Agent-owned ceiling on a data-collector-supplied ``ssot_get`` read timeout
# (YET-2440). Even where the DC clamps its own schedule-limit knob before
# sending, that bound lives in the CALLER; this one is the agent's: it bounds how
# long a single agent worker can be held on one Salesforce GET, whatever any DC
# build asks for. Slow Data Cloud orgs legitimately need well over the 30s
# default for a ``/ssot`` page (observed p99 63-72s across two orgs), so the
# default stays 30 and callers opt in above it.
_SSOT_REQUEST_TIMEOUT_MAX_SECONDS = 300


class _CapturingSession(requests.Session):
    """
    Wraps requests.Session to capture the body and status of every HTTP call
    made through this session, regardless of method, URL, or status code.

    Two use patterns:

    1. **Library-internal capture.** Caller swaps this session into the
       salesforcecdpconnector library's authentication_helper via
       `_attach_capturing_session` so the library's internal HTTP traffic is
       captured. The library raises ``Error('CDP token retrieval failed with
       code N')`` on non-200 a360/token responses and discards the body; on
       200 responses with unexpected payloads it raises ``KeyError`` after
       discarding the body. ``last_exchange_*`` reflects whichever request the
       library last issued before raising, which is the call that caused the
       error.
    2. **Direct capture.** Caller instantiates and uses the session directly
       (e.g. ``list_dataspaces`` makes raw OAuth + SOQL calls). After each call,
       the caller can inspect ``last_exchange_*`` for response detail when
       enriching errors or log records.

    Bodies are pre-serialized to ``str(response.json())`` for ergonomic inclusion
    in log fields; ``_redact_body`` masks ``access_token`` values before the
    body is surfaced.
    """

    def __init__(self) -> None:
        super().__init__()
        self.last_exchange_body: str | None = None
        self.last_exchange_status: int | None = None

    def _capture(self, response: requests.Response) -> None:
        self.last_exchange_status = response.status_code
        try:
            self.last_exchange_body = str(response.json())
        except Exception:
            self.last_exchange_body = response.text[:500]

    def post(self, url: str, **kwargs: Any):  # type: ignore[override]
        response = super().post(url, **kwargs)
        self._capture(response)
        return response

    def get(self, url: str, **kwargs: Any):  # type: ignore[override]
        response = super().get(url, **kwargs)
        self._capture(response)
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


_SENSITIVE_VALUE_PATTERN = re.compile(
    r"(['\"](?:access_token|refresh_token|id_token|signature)['\"]\s*:\s*)['\"][^'\"]*['\"]"
)


def _redact_body(body: str | None) -> str | None:
    """
    Redact sensitive token values from a captured a360/token response body
    string before including it in error messages or log records.

    The body may be stored as str(response.json()) (token values appear as
    Python string literals: 'access_token': 'eyJ...') or as raw response.text
    (double-quoted JSON: "access_token": "eyJ..."). Both single- and
    double-quoted forms of access_token / refresh_token / id_token / signature
    are masked. Redacting prevents accidental credential exposure when an
    unrelated error fires after a successful token exchange.
    """
    if body is None:
        return None
    return _SENSITIVE_VALUE_PATTERN.sub(r"\1'[REDACTED]'", body)


def _resolve_ssot_timeout(timeout: int | None) -> int:
    """
    Coerce a data-collector-supplied ``ssot_get`` read timeout into a safe bound.

    The value crosses the agent boundary inside the DC's operation payload, so it
    is untrusted in the same way ``path`` is — it decides how long one agent worker
    stays pinned on a single Salesforce GET. Anything that is not a positive ``int``
    falls back to the default (``_SSOT_REQUEST_TIMEOUT_DEFAULT_SECONDS``), and the
    agent enforces its OWN ceiling (``_SSOT_REQUEST_TIMEOUT_MAX_SECONDS``) rather
    than trusting the caller to have clamped correctly (YET-2440).

    ``bool`` is rejected explicitly: it subclasses ``int``, so a JSON ``true`` would
    otherwise be accepted as a 1-second timeout.

    Both non-identity paths (unusable value, over-ceiling value) log a warning so
    a rollout can distinguish "older DC sent nothing" from "DC sent something we
    rejected or clamped" — both would otherwise resolve to indistinguishable
    values in the logs (see the ``ssot_timeout`` field on the SSOT GET's
    ``logger.info`` in :meth:`SalesforceDataCloudProxyClient.ssot_get`).
    """
    if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
        if timeout is not None:
            logger.warning(
                "Salesforce Data Cloud: ssot_get timeout override is unusable, "
                "falling back to default",
                extra={
                    "ssot_timeout_requested": repr(timeout),
                    "ssot_timeout_resolved": _SSOT_REQUEST_TIMEOUT_DEFAULT_SECONDS,
                },
            )
        return _SSOT_REQUEST_TIMEOUT_DEFAULT_SECONDS
    if timeout > _SSOT_REQUEST_TIMEOUT_MAX_SECONDS:
        logger.warning(
            "Salesforce Data Cloud: ssot_get timeout override exceeds the agent "
            "ceiling, clamping",
            extra={
                "ssot_timeout_requested": repr(timeout),
                "ssot_timeout_resolved": _SSOT_REQUEST_TIMEOUT_MAX_SECONDS,
            },
        )
        return _SSOT_REQUEST_TIMEOUT_MAX_SECONDS
    return timeout


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


class _RetryingSalesforceDataCloudCursor(SalesforceCDPCursor):
    """SalesforceCDPCursor that retries the four HTTP-bound methods on transient
    network errors. Inherits the rest (description/data state, ``close``,
    ``rollback``, etc.) from the upstream class unchanged.
    """

    @retry(**_RETRY_KWARGS)
    def execute(self, query: Any, params: Any = None) -> None:
        return super().execute(query, params)

    @retry(**_RETRY_KWARGS)
    def fetchall(self) -> Any:
        return super().fetchall()

    @retry(**_RETRY_KWARGS)
    def fetchone(self) -> Any:
        return super().fetchone()

    @retry(**_RETRY_KWARGS)
    def fetchmany(self, size: Any = None) -> Any:
        return super().fetchmany(size)


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

        def noop(*args: Any, **kwargs: Any) -> None:
            pass

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

            # The library swallows the a360 exchange failure (`except Exception:
            # pass` in _get_token) before falling into _renew_token, so by the
            # time raise_on_renewal fires the real HTTP status/body is gone.
            # Capture the exchange traffic so the error and log carry the actual
            # Salesforce rejection (YET-1790).
            capturing = _attach_capturing_session(self)

            def raise_on_renewal(*args: Any, **kwargs: Any) -> NoReturn:
                status = capturing.last_exchange_status if capturing else None
                body = _redact_body(capturing.last_exchange_body if capturing else None)
                logger.warning(
                    "Salesforce Data Cloud: a360/token exchange failed (core-token path)",
                    extra={
                        "dataspace": dataspace,
                        "exchange_status_code": status,
                        "exchange_error_type": _classify_exchange_status(status),
                        "exchange_response_body": body,
                    },
                )
                if status is None:
                    detail = ""
                elif body:
                    detail = f" (HTTP {status}, Salesforce response: {body})"
                else:
                    detail = f" (HTTP {status})"
                raise Exception(
                    "Token exchange failed. The access token may have expired "
                    f"or the dataspace may not exist.{detail}"
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
            # a scoped Data Cloud token.
            super().__init__(
                login_url,
                client_id=client_id,
                client_secret=client_secret,
                dataspace=dataspace,
            )

            # Suppress _revoke_core_token (YET-1546). After the a360 exchange the library
            # revokes the freshly minted core token (_exchange_token -> _revoke_core_token).
            # Salesforce's client-credentials flow reuses a single platform session per
            # connected app, so revoking it invalidates the session the data-collector
            # reuses for its /ssot/* REST and SOAP Metadata calls -> INVALID_SESSION_ID,
            # which silently drops DLO freshness/volume + federation lineage. The
            # short-lived core token expires on its own; no explicit revoke is needed.
            if (
                hasattr(self, "authentication_helper")
                and self.authentication_helper
                and hasattr(self.authentication_helper, "_revoke_core_token")
            ):
                self.authentication_helper._revoke_core_token = noop
            else:
                raise Exception(
                    "salesforce-cdp-connector library has changed. "
                    "Cannot override _revoke_core_token()."
                )

    def cursor(self) -> SalesforceCDPCursor:
        """Override to return a cursor that retries on transient network errors.

        ``SalesforceCDPConnection.cursor()`` would otherwise hand back a vanilla
        ``SalesforceCDPCursor`` whose ``execute``/``fetch*`` methods bubble
        ``urllib3`` ``ProtocolError`` straight up — we want the retry around the
        HTTP-bound calls so a stale pooled connection from Salesforce's edge
        doesn't fail an entire metric-monitor run.
        """
        if self.closed:
            return super().cursor()  # let the upstream raise the same Error
        return _RetryingSalesforceDataCloudCursor(self)


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


# Cerberus schema for the customer-facing self-hosted credentials JSON. Lives
# on the proxy client because Salesforce Data Cloud is not enrolled in CTP —
# the factory function in proxy_client_factory.py inlines the field reads
# (domain/client_id/client_secret/[core_token]/[refresh_token]/[dataspace]).
SALESFORCE_DATA_CLOUD_CREDENTIALS_SCHEMA: dict[str, Any] = {
    "connect_args": {
        "type": "dict",
        "required": True,
        "schema": {
            "domain": {"type": "string", "required": True, "empty": False},
            "client_id": {"type": "string", "required": True, "empty": False},
            "client_secret": {"type": "string", "required": True, "empty": False},
            "core_token": {"type": "string"},
            "refresh_token": {"type": "string"},
            "dataspace": {"type": "string"},
        },
    },
}


class SalesforceDataCloudProxyClient(BaseDbProxyClient):
    SELF_HOSTED_CREDENTIALS_SCHEMA: dict[str, Any] = (
        SALESFORCE_DATA_CLOUD_CREDENTIALS_SCHEMA
    )

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

    def _close_client(self):
        # Guard against a never-established connection (e.g. teardown via
        # __del__ after a failed __init__).
        if self._connection:
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
                tables: list[GenieTable] = retry_call(conn.list_tables, **_RETRY_KWARGS)
            except SalesforceCDPError as e:
                body = _redact_body(capturing.last_exchange_body if capturing else None)
                status = capturing.last_exchange_status if capturing else None
                # The CDP library raises SalesforceCDPError for BOTH the a360 token exchange AND
                # the subsequent getAllMetadata query. A post-exchange metadata-query failure
                # (the token was obtained, then e.g. a transient DEADLINE_EXCEEDED) must NOT be
                # reported as a token-exchange / permission problem — that sends triage down the
                # wrong path (YET-1631).
                err_text = str(e)
                err_text_lower = err_text.lower()
                is_metadata_query_failure = (
                    "executing metadata query" in err_text_lower
                    or "getallmetadata" in err_text_lower
                )
                if is_metadata_query_failure:
                    # The token exchange succeeded, so the captured `body` is the token-exchange
                    # response — NOT the metadata-query response. Don't append it to this message
                    # (it would misrepresent what the body is); it's still logged below as
                    # `exchange_response_body`, where the label is accurate.
                    logger.warning(
                        "Salesforce Data Cloud: metadata query failed for dataspace '%s' "
                        "(token exchange succeeded)",
                        dataspace,
                        extra={
                            "dataspace": dataspace,
                            "exchange_status_code": status,
                            "metadata_query_error": err_text[:500],
                            "exchange_response_body": body,
                        },
                    )
                    raise RuntimeError(
                        f"Metadata query failed for dataspace '{dataspace}': {e}"
                    ) from e
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
            #
            # Preserve the original auth tokens (core_token / refresh_token) so we
            # reuse any pre-fetched credentials rather than forcing an unnecessary
            # client-credentials re-flow.  Only dataspace is cleared to remove scoping.
            if self._credentials.dataspace:
                unscoped_conn: SalesforceDataCloudConnection | None = (
                    SalesforceDataCloudConnection(
                        f"https://{self._credentials.domain}",
                        client_id=self._credentials.client_id,
                        client_secret=self._credentials.client_secret,
                        core_token=self._credentials.core_token,
                        refresh_token=self._credentials.refresh_token,
                        dataspace=None,
                    )
                )
            else:
                unscoped_conn = None
            conn_to_use = unscoped_conn or self._connection
            try:
                tables = retry_call(conn_to_use.list_tables, **_RETRY_KWARGS)
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

    def list_dataspaces(self) -> list[str]:
        """
        Discover Data Cloud dataspaces accessible to the run-as user via SOQL.

        Mints a core OAuth token via the client-credentials grant against the
        Salesforce core REST API (the ``salesforcecdpconnector`` library does
        not expose its token-minting flow for arbitrary SOQL), then issues::

            GET /services/data/{version}/query?q=SELECT DataSpaceApiName FROM Dataspace

        with the core token as a Bearer credential. Pagination via
        ``nextRecordsUrl`` is handled defensively even though the Dataspace
        SObject typically returns a small result set.

        The result is scoped to the integration user's Data Cloud permissions
        in Salesforce. An empty list means the run-as user has no dataspace
        access, not that the org has no dataspaces — surface the warning in
        the caller (the data-collector) when this matters.

        Raises ``RuntimeError`` with the HTTP status in the ``code NNN`` format
        on any non-200 response. Both HTTP calls flow through a single
        ``_CapturingSession`` so the redacted response body is surfaced in the
        exception message and structured log record on failure.
        """
        domain = self._credentials.domain
        session = _CapturingSession()

        logger.debug(
            "Salesforce Data Cloud: discovering dataspaces via SOQL "
            f"(domain={domain}, client_id={self._credentials.client_id[:8]}...)",
        )

        # 1. Mint a core OAuth token via the client-credentials grant.
        access_token = self._mint_core_token(session)

        # 2. Issue SOQL query for the Dataspace SObject; follow nextRecordsUrl pagination.
        dataspaces: list[str] = []
        url: str | None = f"https://{domain}/services/data/{_SOQL_API_VERSION}/query"
        params: dict[str, str] | None = {"q": _LIST_DATASPACES_SOQL}

        while url is not None:
            response = session.get(
                url,
                headers={"Authorization": f"Bearer {access_token}"},
                params=params,
                timeout=_DISCOVERY_REQUEST_TIMEOUT_SECONDS,
            )
            if response.status_code != 200:
                body = _redact_body(session.last_exchange_body)
                status = session.last_exchange_status
                logger.warning(
                    "Salesforce Data Cloud: SOQL dataspace discovery failed",
                    extra={
                        "exchange_status_code": status,
                        "exchange_error_type": _classify_exchange_status(status),
                        "exchange_response_body": body,
                    },
                )
                detail = f" (Salesforce response: {body})" if body else ""
                raise RuntimeError(
                    f"Salesforce Data Cloud dataspace discovery: SOQL query failed "
                    f"with code {response.status_code}{detail}"
                )

            try:
                payload = response.json()
            except ValueError as e:
                body = _redact_body(session.last_exchange_body)
                raise RuntimeError(
                    f"Salesforce Data Cloud dataspace discovery: non-JSON response "
                    f"(HTTP {response.status_code}, Salesforce response: {body})"
                ) from e

            for record in payload.get("records", []):
                name = record.get("DataSpaceApiName")
                if name:
                    dataspaces.append(name)

            if payload.get("done", True):
                url = None
            else:
                next_records_url = payload.get("nextRecordsUrl")
                if not next_records_url:
                    raise RuntimeError(
                        "Salesforce Data Cloud dataspace discovery: done=False but "
                        "no nextRecordsUrl in response"
                    )
                # nextRecordsUrl already encodes the cursor in the path.
                url = f"https://{domain}{next_records_url}"
                params = None

        logger.info(
            "Salesforce Data Cloud: discovered dataspaces via SOQL",
            extra={
                "discovered_dataspace_count": len(dataspaces),
                # Lists/dicts may be redacted in the log pipeline; keep as a string.
                "discovered_dataspaces_csv": ", ".join(dataspaces),
            },
        )
        return dataspaces

    def _mint_core_token(self, session: _CapturingSession) -> str:
        """
        Mint a short-lived core OAuth token via the Salesforce client-credentials
        grant (``POST /services/oauth2/token`` on the org's My Domain) and return
        the access token.

        This token authenticates Salesforce **core REST** calls
        (``/services/data/...``) as the connected app's run-as user. The
        ``salesforcecdpconnector`` library mints its own token internally for Data
        Cloud query traffic but does not expose that flow for arbitrary core REST
        calls, so dataspace discovery (:meth:`list_dataspaces`) and generic SSOT
        reads (:meth:`ssot_get`) mint their own token here.

        ``session`` is a :class:`_CapturingSession` so the caller can surface the
        redacted response body on failure. Raises ``RuntimeError`` carrying
        ``code NNN`` on a non-200 response, and ``HTTP NNN`` on a 200 payload
        missing ``access_token``. The token itself is never logged.
        """
        domain = self._credentials.domain
        token_url = f"https://{domain}/services/oauth2/token"
        token_response = session.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self._credentials.client_id,
                "client_secret": self._credentials.client_secret,
            },
            timeout=_DISCOVERY_REQUEST_TIMEOUT_SECONDS,
        )
        if token_response.status_code != 200:
            body = _redact_body(session.last_exchange_body)
            status = session.last_exchange_status
            logger.warning(
                "Salesforce Data Cloud: OAuth core token mint failed",
                extra={
                    "exchange_status_code": status,
                    "exchange_error_type": _classify_exchange_status(status),
                    "exchange_response_body": body,
                },
            )
            detail = f" (Salesforce response: {body})" if body else ""
            raise RuntimeError(
                f"Salesforce Data Cloud: OAuth core token mint failed with code "
                f"{token_response.status_code}{detail}"
            )
        try:
            return token_response.json()["access_token"]
        except (ValueError, KeyError) as e:
            body = _redact_body(session.last_exchange_body)
            raise RuntimeError(
                f"Salesforce Data Cloud: OAuth response missing access_token "
                f"(HTTP {token_response.status_code}, Salesforce response: {body})"
            ) from e

    def ssot_get(self, path: str, timeout: int | None = None) -> dict | list:
        """
        Issue an authenticated GET against a Salesforce **core REST** path on the
        connection's My Domain and return the parsed JSON body.

        The body is returned as-is from ``response.json()`` — usually a JSON
        object (``dict``), but occasionally a top-level JSON list for some core
        REST endpoints.

        This is the generic primitive the data-collector uses to read the
        Salesforce SSOT metadata endpoints
        (``/services/data/{version}/ssot/*`` — data streams, catalogs, schemas,
        ...). The data-collector supplies the path to append; the agent owns
        building the URL from the connection's credentials and authenticating the
        call, so the customer's token never leaves the agent — only the JSON body
        is returned.

        Authentication mirrors :meth:`list_dataspaces`: a short-lived core token
        is minted via the client-credentials grant (:meth:`_mint_core_token`) and
        sent as a Bearer credential. SSOT endpoints are core REST on the My Domain
        authenticated with the **core token** — not the Data Cloud query token
        obtained via the a360 exchange.

        ``path`` must be a relative path beginning with ``/`` (e.g.
        ``/services/data/v62.0/ssot/data-streams``). Values carrying a scheme or
        host (absolute or protocol-relative URLs), or embedded CR/LF, are
        rejected so the minted token is only ever sent to the connection's own
        My Domain. Rejection is based on the parsed URL structure, so a query
        string that merely embeds a URL (e.g. a pagination cursor) is allowed.

        ``timeout`` optionally overrides the READ bound on the Salesforce GET
        (default ``_SSOT_REQUEST_TIMEOUT_DEFAULT_SECONDS``, 30s); it is coerced and
        clamped by :func:`_resolve_ssot_timeout`, whose two branches have OPPOSITE
        consequences: a non-``int``/``bool``/non-positive value DEGRADES to the
        30s default, while a value above the agent's ceiling is CLAMPED DOWN to
        that ceiling (300s) rather than degrading to 30. The override covers
        **only** the read bound on this GET — the connect phase stays pinned at
        the discovery bound (see the tuple ``timeout`` passed to ``session.get``
        below), and the client-credentials token mint that precedes it keeps its
        own 30s bound — so a caller sizing a transport deadman must budget
        ``30 + timeout``, not ``timeout`` (YET-2440).

        Raises ``ValueError`` for an unsafe ``path``; ``RuntimeError`` carrying
        ``code NNN`` on a non-200 response and ``HTTP NNN`` on a non-JSON 200
        response, with the response body redacted before it is surfaced.
        """
        validate_relative_rest_path(path)
        request_timeout = _resolve_ssot_timeout(timeout)
        split = urllib.parse.urlsplit(path)
        # Query-string values (pagination cursors, filters) may be sensitive —
        # logs and error messages only ever carry the path component.
        log_path = split.path
        has_query = bool(split.query)

        domain = self._credentials.domain
        session = _CapturingSession()

        logger.info(
            "Salesforce Data Cloud: SSOT GET "
            f"(domain={domain}, path={log_path}, has_query={has_query}, "
            f"client_id={self._credentials.client_id[:8]}...)",
            extra={
                "ssot_path": log_path,
                "ssot_has_query": has_query,
                "ssot_timeout": request_timeout,
            },
        )

        access_token = self._mint_core_token(session)

        # ``requests`` applies a scalar ``timeout`` to the connect phase AND the
        # read phase independently, so passing ``request_timeout`` alone would
        # silently widen the TCP/TLS connect bound from the discovery default up
        # to whatever the caller asked for (up to 300s) — verified empirically:
        # a scalar ``timeout=3`` against a black-holed IP raises ConnectTimeout at
        # 3.01s, while a tuple ``(1, 30)`` raises it at 1.00s. Pin connect at the
        # discovery bound via a tuple; only the read bound is caller-tunable.
        response = session.get(
            f"https://{domain}{path}",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=(_DISCOVERY_REQUEST_TIMEOUT_SECONDS, request_timeout),
        )
        if response.status_code != 200:
            body = _redact_body(session.last_exchange_body)
            status = session.last_exchange_status
            # A 404 is the expected, benign response from orgs that lack the SSOT
            # feature behind the requested path (e.g. Data Governance tags on orgs
            # without the license, YET-1998). It still raises `code 404` below so
            # the data-collector can branch on status and skip silently, but it
            # must NOT warn — orgs without the feature would otherwise spam a
            # warning every collection cycle. Genuinely unexpected non-200s
            # (auth, rate limits, 5xx, ...) still warn. Key the demotion off the
            # same captured `status` reported in `extra` so the log level always
            # matches the status we surface.
            log = logger.debug if status == 404 else logger.warning
            log(
                "Salesforce Data Cloud: SSOT GET failed",
                extra={
                    "ssot_path": log_path,
                    "ssot_has_query": has_query,
                    "exchange_status_code": status,
                    "exchange_error_type": _classify_exchange_status(status),
                    "exchange_response_body": body,
                },
            )
            detail = f" (Salesforce response: {body})" if body else ""
            raise RuntimeError(
                f"Salesforce Data Cloud SSOT GET {log_path} failed with code "
                f"{response.status_code}{detail}"
            )

        try:
            return response.json()
        except ValueError as e:
            body = _redact_body(session.last_exchange_body)
            raise RuntimeError(
                f"Salesforce Data Cloud SSOT GET {log_path}: non-JSON response "
                f"(HTTP {response.status_code}, Salesforce response: {body})"
            ) from e

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
