from typing import Any, Dict, List, Optional, Tuple

from apollo.agent.utils import AgentUtils
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.http.http_proxy_client import HttpProxyClient

_ATTR_CONNECT_ARGS = "connect_args"
_WORKSPACE_URL_KEY = "databricks_workspace_url"


class DatabricksRestProxyClient(BaseProxyClient):
    """
    Proxy client for Databricks REST API calls.

    Token is resolved by the CTP resolve_databricks_token transform (supports PAT,
    Databricks-managed OAuth, and Azure-managed OAuth) and passed through connect_args.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        creds = credentials["connect_args"] if credentials else {}
        self._http_client = HttpProxyClient(credentials={"token": creds.get("token")})
        self._databricks_workspace_url: Optional[str] = creds.get(_WORKSPACE_URL_KEY)

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
        if not self._databricks_workspace_url:
            raise ValueError(f"Databricks workspace URL not found in credentials")
        url = AgentUtils.normalize_url(
            self._databricks_workspace_url, url
        )  # url is actually the path
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
