"""Microsoft identity platform (MSAL) token acquisition for the Power BI connection.

The Power BI agent connection talks to two Azure-AD-protected hosts using the *same*
service-principal / primary-user credentials but *different* token audiences (scopes):

- ``api.powerbi.com``          → the Power BI REST API (reports / metadata scan — existing)
- ``api.fabric.microsoft.com`` → the Microsoft Fabric API (Gen2 CI/CD dataflows — YET-2063)

A single minted token can only carry one audience, so the proxy client selects the scope per
request by destination host and mints/caches a token per scope via :class:`PowerBiTokenProvider`.
The scope-parameterized :func:`acquire_token` is shared with the ``resolve_msal_token`` CTP
transform so the MSAL acquisition logic lives in exactly one place.
"""

from typing import Dict, List, Optional, Tuple

import msal

_AUTHORITY_URL_PREFIX = "https://login.microsoftonline.com/"

# Token audiences (scopes). Same credentials, different audience per host.
POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"

# Destination hosts the agent injects a token for. Any other host → no token.
POWERBI_HOST = "api.powerbi.com"
FABRIC_HOST = "api.fabric.microsoft.com"

# Host → scope list. The proxy client keys token selection off this map; a host absent from it
# gets no Authorization header (the contract agreed with data-collector PR #2523).
HOST_SCOPES: Dict[str, List[str]] = {
    POWERBI_HOST: [POWERBI_SCOPE],
    FABRIC_HOST: [FABRIC_SCOPE],
}

AUTH_MODE_SERVICE_PRINCIPAL = "service_principal"
AUTH_MODE_PRIMARY_USER = "primary_user"
SUPPORTED_AUTH_MODES = (AUTH_MODE_SERVICE_PRINCIPAL, AUTH_MODE_PRIMARY_USER)


class MsalAuthError(Exception):
    """MSAL token acquisition failed — unsupported auth mode, missing required field, or an
    error/empty response from Azure AD. Surfaced to the caller as a clear error rather than a
    generic 500 (e.g. a Fabric tenant with the SP not yet authorized returns an AAD error here).
    """


def acquire_token(
    auth_mode: Optional[str],
    client_id: Optional[str],
    tenant_id: Optional[str],
    scopes: List[str],
    client_secret: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> str:
    """Acquire an MSAL access token for ``scopes`` using the given credentials.

    ``auth_mode`` selects the flow: ``service_principal`` (client-credentials grant) or
    ``primary_user`` (username/password). The same credentials work for any Azure-AD audience;
    ``scopes`` chooses which one. Raises :class:`MsalAuthError` on validation or acquisition
    failure.
    """
    if auth_mode not in SUPPORTED_AUTH_MODES:
        raise MsalAuthError(
            f"Unsupported auth_mode: '{auth_mode}'. Expected one of: {SUPPORTED_AUTH_MODES}"
        )
    if not client_id:
        raise MsalAuthError("'client_id' is required for MSAL token acquisition")
    if not tenant_id:
        raise MsalAuthError("'tenant_id' is required for MSAL token acquisition")

    authority = f"{_AUTHORITY_URL_PREFIX}{tenant_id}"

    if auth_mode == AUTH_MODE_SERVICE_PRINCIPAL:
        if not client_secret:
            raise MsalAuthError(
                "'client_secret' is required for service_principal auth_mode"
            )
        return _service_principal_token(client_id, client_secret, authority, scopes)

    if not username:
        raise MsalAuthError("'username' is required for primary_user auth_mode")
    if not password:
        raise MsalAuthError("'password' is required for primary_user auth_mode")
    return _primary_user_token(client_id, authority, username, password, scopes)


def _service_principal_token(
    client_id: str, client_secret: str, authority: str, scopes: List[str]
) -> str:
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority,
    )
    response = app.acquire_token_for_client(scopes=scopes)
    _raise_for_response(response)
    assert response is not None
    return response["access_token"]


def _primary_user_token(
    client_id: str,
    authority: str,
    username: str,
    password: str,
    scopes: List[str],
) -> str:
    app = msal.PublicClientApplication(client_id, authority=authority)
    accounts = app.get_accounts(username=username)

    response: Optional[dict] = None
    if accounts:
        response = app.acquire_token_silent(scopes=scopes, account=accounts[0])
    if not response:
        response = app.acquire_token_by_username_password(
            username=username, password=password, scopes=scopes
        )

    _raise_for_response(response)
    assert response is not None
    return response["access_token"]


def _raise_for_response(response: Optional[dict]) -> None:
    if not response:
        raise MsalAuthError("MSAL acquire token response is empty")
    error = response.get("error")
    if error:
        raise MsalAuthError(
            f"MSAL error: {error} ({response.get('error_description')})"
        )


class PowerBiTokenProvider:
    """Resolves the correct scoped bearer token for a Power BI request by destination host.

    Two construction modes, distinguished by the resolved ``connect_args``:

    - **raw-creds mode** (``auth_mode`` present): mints a token per scope on demand from the
      service-principal / primary-user credentials, so both the Power BI and Fabric audiences
      are available from the same credentials.
    - **pre-shaped mode** (a ``token`` present, no ``auth_mode``): a token already minted by the
      data-collector (rare, self-hosted). It is Power BI-scoped, so it is served only for
      ``POWERBI_HOST``; other hosts (including Fabric) get nothing, since there are no
      credentials to mint a different audience.

    Tokens are cached per scope for the provider's lifetime — one provider per proxy-client
    instance — mirroring the data-collector's instance-scoped per-scope cache.
    """

    def __init__(self, connect_args: Optional[Dict]):
        connect_args = connect_args or {}
        self._auth_mode: Optional[str] = connect_args.get("auth_mode")
        self._client_id: Optional[str] = connect_args.get("client_id")
        self._tenant_id: Optional[str] = connect_args.get("tenant_id")
        self._client_secret: Optional[str] = connect_args.get("client_secret")
        self._username: Optional[str] = connect_args.get("username")
        self._password: Optional[str] = connect_args.get("password")
        # Legacy pre-shaped Power BI-scoped token (no raw creds to mint with).
        self._preshaped_token: Optional[str] = connect_args.get("token")
        # scope-key (sorted tuple of scopes) → minted token
        self._tokens: Dict[Tuple[str, ...], str] = {}

    def token_for_host(self, host: Optional[str]) -> Optional[str]:
        """Return the bearer token to inject for ``host``, or ``None`` when the host is not one
        we authenticate to (contract: any other host → no token). Raises :class:`MsalAuthError`
        if minting is required but fails."""
        scopes = HOST_SCOPES.get(host or "")
        if not scopes:
            return None
        if self._auth_mode:
            return self._minted_token(scopes)
        # Pre-shaped mode: the token is Power BI-scoped, so serve it only for the Power BI host.
        if host == POWERBI_HOST:
            return self._preshaped_token
        return None

    def _minted_token(self, scopes: List[str]) -> str:
        cache_key = tuple(sorted(scopes))
        token = self._tokens.get(cache_key)
        if token is None:
            token = acquire_token(
                auth_mode=self._auth_mode,
                client_id=self._client_id,
                tenant_id=self._tenant_id,
                scopes=scopes,
                client_secret=self._client_secret,
                username=self._username,
                password=self._password,
            )
            self._tokens[cache_key] = token
        return token
