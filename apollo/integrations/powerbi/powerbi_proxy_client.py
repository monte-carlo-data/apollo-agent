import logging
from typing import (
    Any,
    Dict,
    Optional,
)

import msal

from apollo.common.agent.models import AgentError
from apollo.integrations.http.http_proxy_client import HttpProxyClient

logger = logging.getLogger(__name__)


def _auth_as_service_principal(credentials: Dict) -> str:
    clientapp = msal.ConfidentialClientApplication(
        client_id=credentials.get("client_id"),
        client_credential=credentials.get("client_secret"),
        authority=f'{_AUTHORITY_URL_PREFIX}{credentials.get("tenant_id")}',
    )
    response = clientapp.acquire_token_for_client(scopes=_SCOPES)
    _raise_for_acquire_token_response(response)
    return response.get("access_token", "") if response else ""


def _auth_as_primary_user(credentials: Dict) -> str:
    client_id = credentials.get("client_id")
    authority = f'{_AUTHORITY_URL_PREFIX}{credentials.get("tenant_id")}'

    # Create a public client to authorize the app with the AAD app
    clientapp = msal.PublicClientApplication(client_id, authority=authority)

    username = credentials.get("username")
    password = credentials.get("password")
    accounts = clientapp.get_accounts(username=username)

    response = None
    if accounts:
        # Retrieve Access token from user cache if available
        response = clientapp.acquire_token_silent(scopes=_SCOPES, account=accounts[0])

    if not response:
        # Make a client call if Access token is not available in cache
        response = clientapp.acquire_token_by_username_password(
            username=username, password=password, scopes=_SCOPES
        )

    _raise_for_acquire_token_response(response)
    return response.get("access_token", "")


def _get_access_token(credentials: Dict) -> Optional[str]:
    """
    Generates access token from the connection
    """
    if not credentials:
        return None

    auth_mode = credentials.get("auth_mode", "")
    if auth_mode not in _AUTH_FUNCTIONS:
        return None
    auth_func = _AUTH_FUNCTIONS[auth_mode]
    return auth_func(credentials=credentials)


def _raise_for_acquire_token_response(response: Optional[Dict]):
    if not response:
        raise AgentError("Acquire token response is empty")
    error = response.get("error")
    if error:
        error_description = response.get("error_description")
        raise AgentError(f"Azure Active Directory error: {error} ({error_description})")


_AUTH_MODE_SERVICE_PRINCIPAL = "service_principal"
_AUTH_MODE_PRIMARY_USER = "primary_user"
_AUTH_FUNCTIONS = {
    _AUTH_MODE_SERVICE_PRINCIPAL: _auth_as_service_principal,
    _AUTH_MODE_PRIMARY_USER: _auth_as_primary_user,
}

_AUTHORITY_URL_PREFIX = "https://login.microsoftonline.com/"
_SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]


class PowerBiProxyClient(HttpProxyClient):
    """
    PowerBI Proxy Client, simple class that uses the received credentials to create an auth
    token. This auth token is used to authenticate subsequent http requests to PowerBI.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):  # noqa
        if not credentials:
            raise ValueError("Credentials are required for PowerBI")

        super().__init__(
            credentials={
                **credentials,
                **dict(
                    auth_type="Bearer",
                    token=_get_access_token(credentials),
                ),
            }
        )
