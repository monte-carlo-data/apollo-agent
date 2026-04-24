from typing import Any, Dict, List, Optional, Tuple

from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.http.http_proxy_client import HttpProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class InformaticaProxyClient(BaseProxyClient):
    """
    Proxy client for Informatica Cloud connections.

    Expects connect_args (produced by the CTP pipeline, specifically the
    resolve_informatica_session transform) to contain a pre-resolved session:

        connect_args:
            session_id    (required): icSessionId from the Informatica login response
            api_base_url  (required): API base URL from the Informatica login response

    All authentication — V2 password, V3 password, or JWT loginOAuth — is handled
    upstream in the CTP pipeline. This client is auth-method agnostic.

    The icSessionId header is always used for API calls regardless of how the session
    was obtained. This is intentional: the DC currently calls only V2 API endpoints,
    which require icSessionId (not INFA-SESSION-ID).
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Informatica agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        connect_args: Dict[str, Any] = credentials[_ATTR_CONNECT_ARGS]

        session_id = connect_args.get("session_id")
        api_base_url = connect_args.get("api_base_url")

        if not session_id:
            raise ValueError(
                "Informatica agent client requires 'session_id' in connect_args"
            )
        if not api_base_url:
            raise ValueError(
                "Informatica agent client requires 'api_base_url' in connect_args"
            )

        self._api_base_url: str = api_base_url.rstrip("/")

        # icSessionId is used for all API calls — this is coupled to V2 API usage,
        # not the auth method used to obtain the session.
        http_credentials: Dict[str, Any] = {
            "token": session_id,
            "auth_header": "icSessionId",
            "auth_type": "",  # empty string → send token as-is, no "Bearer " prefix
        }
        self._http_client = HttpProxyClient(credentials=http_credentials)

    @property
    def wrapped_client(self):
        return None

    def do_http_request(
        self,
        path: str,
        http_method: str = "GET",
        payload: Optional[Dict] = None,
        content_type: Optional[str] = None,
        timeout: Optional[int] = None,
        additional_headers: Optional[Dict] = None,
        params: Optional[Dict] = None,
        retry_status_code_ranges: Optional[List[Tuple]] = None,
        data: Optional[str] = None,
    ) -> Any:
        """
        Execute an HTTP request against the Informatica API base URL.

        :param path: Path to append to the API base URL (e.g., "/v2/jobs"). Must start with "/".
        :param http_method: HTTP method (GET, POST, PUT, DELETE, etc.)
        :param payload: Optional JSON payload for the request body
        :param content_type: Optional Content-Type header value
        :param timeout: Optional timeout in seconds
        :param additional_headers: Optional additional headers
        :param params: Optional query string parameters
        :param retry_status_code_ranges: Optional status code ranges that trigger retry
        :param data: Optional raw data for the request body
        :return: JSON response (dict or list) or empty dict for empty responses
        """
        if not path.startswith("/"):
            path = f"/{path}"
        url = f"{self._api_base_url}{path}"

        return self._http_client.do_request(
            url=url,
            http_method=http_method,
            payload=payload,
            content_type=content_type,
            timeout=timeout,
            additional_headers=additional_headers,
            params=params,
            retry_status_code_ranges=retry_status_code_ranges,
            data=data,
        )

    def get_error_type(self, error: Exception) -> Optional[str]:
        http_error_type = self._http_client.get_error_type(error)
        if http_error_type:
            return http_error_type
        return super().get_error_type(error=error)

    def get_error_extra_attributes(self, error: Exception) -> Optional[Dict]:
        http_attrs = self._http_client.get_error_extra_attributes(error)
        if http_attrs:
            return http_attrs
        return super().get_error_extra_attributes(error=error)
