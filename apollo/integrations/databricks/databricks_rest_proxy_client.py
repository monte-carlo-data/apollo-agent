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

    def _get_token(self, creds: Dict) -> Optional[str]:
        client_id = creds.get(_CLIENT_ID_KEY)
        client_secret = creds.get(_CLIENT_SECRET_KEY)

        if client_id and client_secret:
            host = creds.get(_WORKSPACE_URL_KEY, "")
            azure_tenant_id = creds.get(_AZURE_TENANT_ID_KEY)
            azure_workspace_resource_id = creds.get(_AZURE_WORKSPACE_RESOURCE_ID_KEY)

            if azure_tenant_id and azure_workspace_resource_id:
                config = Config(
                    host=host,
                    azure_client_id=client_id,
                    azure_client_secret=client_secret,
                    azure_tenant_id=azure_tenant_id,
                    azure_workspace_resource_id=azure_workspace_resource_id,
                )
                provider = azure_service_principal
            else:
                config = Config(
                    host=host,
                    client_id=client_id,
                    client_secret=client_secret,
                )
                provider = oauth_service_principal

            # provider(config) returns a HeaderFactory callable: () -> Dict[str, str]
            header_factory = provider(config)
            headers = header_factory()
            auth_header = headers.get("Authorization", "")
            # Strip "Bearer " prefix to get the raw token
            return auth_header.removeprefix("Bearer ").strip() or None

        return creds.get(_TOKEN_KEY)

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
