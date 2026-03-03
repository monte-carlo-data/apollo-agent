from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from databricks.sdk.core import Config, azure_service_principal, oauth_service_principal

from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.http.http_proxy_client import HttpProxyClient

_ATTR_CONNECT_ARGS = "connect_args"
_WORKSPACE_URL_KEY = "databricks_workspace_url"
_TOKEN_KEY = "databricks_token"
_CLIENT_ID_KEY = "databricks_client_id"
_CLIENT_SECRET_KEY = "databricks_client_secret"
_AZURE_TENANT_ID_KEY = "azure_tenant_id"
_AZURE_WORKSPACE_RESOURCE_ID_KEY = "azure_workspace_resource_id"


class AuthenticationMode(Enum):
    TOKEN = "token"
    """Auth with a Personal Access Token"""
    AZURE_OAUTH = "azure_oauth"
    """OAuth with an Azure Entra ID managed service principal"""
    DATABRICKS_OAUTH = "databricks_oauth"
    """OAuth with a Databricks managed service principal"""


class DatabricksRestProxyClient(BaseProxyClient):
    """
    Proxy client for Databricks REST API calls.

    Accepts credentials in flat format or wrapped under "connect_args" (for self-hosted
    credentials where the agent resolves the actual values from a secrets manager).

    Supports PAT, Databricks-managed OAuth, and Azure-managed OAuth authentication.
    Generates the auth token at initialization time and delegates REST calls to
    HttpProxyClient.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        # Support both flat and connect_args-wrapped formats, matching BqProxyClient pattern
        creds: Dict = {}
        if credentials:
            creds = dict(credentials.get(_ATTR_CONNECT_ARGS, credentials))

        token = self._get_token(creds)
        self._http_client = HttpProxyClient(credentials={"token": token})

    def _authentication_mode(self, creds: Dict) -> AuthenticationMode:
        # IMPORTANT: check for OAuth related creds first. Customers
        # who migrated from PAT to OAuth might have the old PAT
        # in the credentials dict still.
        if creds.get(_CLIENT_ID_KEY) and creds.get(_CLIENT_SECRET_KEY):
            if creds.get(_AZURE_TENANT_ID_KEY) and creds.get(
                _AZURE_WORKSPACE_RESOURCE_ID_KEY
            ):
                return AuthenticationMode.AZURE_OAUTH
            return AuthenticationMode.DATABRICKS_OAUTH
        if creds.get(_TOKEN_KEY):
            return AuthenticationMode.TOKEN
        raise RuntimeError("No supported credentials mode found.")

    def _get_token(self, creds: Dict) -> Optional[str]:
        auth_mode = self._authentication_mode(creds)

        if auth_mode == AuthenticationMode.DATABRICKS_OAUTH:
            config = Config(
                host=creds.get(_WORKSPACE_URL_KEY, ""),
                client_id=creds.get(_CLIENT_ID_KEY),
                client_secret=creds.get(_CLIENT_SECRET_KEY),
            )
            provider = oauth_service_principal
        elif auth_mode == AuthenticationMode.AZURE_OAUTH:
            config = Config(
                host=creds.get(_WORKSPACE_URL_KEY, ""),
                azure_client_id=creds.get(_CLIENT_ID_KEY),
                azure_client_secret=creds.get(_CLIENT_SECRET_KEY),
                azure_tenant_id=creds.get(_AZURE_TENANT_ID_KEY),
                azure_workspace_resource_id=creds.get(_AZURE_WORKSPACE_RESOURCE_ID_KEY),
            )
            provider = azure_service_principal
        else:
            return creds.get(_TOKEN_KEY)

        # provider(config) returns a HeaderFactory callable: () -> Dict[str, str]
        header_factory = provider(config)
        auth_header = header_factory().get("Authorization", "")
        # Strip "Bearer " prefix to get the raw token
        return auth_header.removeprefix("Bearer ").strip() or None

    @property
    def wrapped_client(self):
        return None

    def do_request(
        self,
        url: str,
        http_method: str = "GET",
        payload: Optional[Dict] = None,
        timeout: Optional[int] = None,
        user_agent: Optional[str] = None,
        additional_headers: Optional[Dict] = None,
        params: Optional[Dict] = None,
        retry_status_code_ranges: Optional[List[Tuple]] = None,
        **kwargs: Any,
    ) -> Dict:
        return self._http_client.do_request(
            url=url,
            http_method=http_method,
            payload=payload,
            timeout=timeout,
            user_agent=user_agent,
            additional_headers=additional_headers,
            params=params,
            retry_status_code_ranges=retry_status_code_ranges,
        )

    def get_error_type(self, error: Exception) -> Optional[str]:
        return self._http_client.get_error_type(error)

    def get_error_extra_attributes(self, error: Exception) -> Optional[Dict]:
        return self._http_client.get_error_extra_attributes(error)
