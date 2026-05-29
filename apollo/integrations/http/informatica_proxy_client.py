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
            session_id    (required): session token from the Informatica login response
            api_base_url  (required): API base URL from the Informatica login response

    All authentication — V2 password, V3 password, or JWT loginOAuth — is handled
    upstream in the CTP pipeline. This client is auth-method agnostic.

    The Informatica session header is selected per request based on the path:

    - V3 endpoints (``/public/core/v3/...``) read the token from ``INFA-SESSION-ID``.
    - Any other path falls back to ``icSessionId`` (which is what V2
      ``/api/v2/...`` endpoints expect).

    Each request carries only the header its endpoint actually reads — no
    redundant auth header on the wire.
    """

    _V3_PATH_PREFIX = "/public/core/v3/"

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
        self._session_id: str = session_id

        # The Informatica session header is path-dependent (V2 vs V3), so we
        # don't rely on HttpProxyClient's auto-attached auth header. The right
        # header is injected per request in ``do_http_request`` instead.
        self._http_client = HttpProxyClient(credentials=None)

    @property
    def wrapped_client(self):
        return None

    def get_connection_metadata(self) -> Dict[str, Any]:
        """Expose the CTP-resolved API base URL so the Data Collector can
        construct customer-facing run-detail links — the DC has no other way
        to discover the resolved POD URL when running through an agent.
        """
        return {"api_base_url": self._api_base_url}

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

        # Pick the session header for the API surface this path targets.
        # Caller-supplied additional_headers win on collision so a deliberate
        # override (e.g., for an unusual endpoint) isn't silently masked.
        if path.startswith(self._V3_PATH_PREFIX):
            session_header = "INFA-SESSION-ID"
        else:
            session_header = "icSessionId"
        merged_headers: Dict[str, Any] = {session_header: self._session_id}
        if additional_headers:
            merged_headers.update(additional_headers)

        return self._http_client.do_request(
            url=url,
            http_method=http_method,
            payload=payload,
            content_type=content_type,
            timeout=timeout,
            additional_headers=merged_headers,
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
