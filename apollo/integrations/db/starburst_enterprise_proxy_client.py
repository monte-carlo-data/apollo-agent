import base64
import hashlib
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import trino

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient, SslOptions
from apollo.integrations.http.http_proxy_client import HttpProxyClient

logger = logging.getLogger(__name__)

_ATTR_CONNECT_ARGS = "connect_args"


class StarburstEnterpriseProxyClient(BaseDbProxyClient):
    """
    Proxy client for Starburst Enterprise (self-hosted) connections.

    Extends BaseDbProxyClient with:
    - Trino DB-API connection (same as StarburstProxyClient)
    - HTTP request capabilities for accessing Starburst REST APIs
      (e.g., Data Products API) using the host/port from credentials

    This is needed for self-hosted scenarios where the Data Collector doesn't know
    the host/port until the agent receives the credentials.
    """

    def __init__(self, credentials: Optional[Dict], platform: str, **kwargs: Any):
        super().__init__(connection_type="starburst-enterprise")
        self._platform = platform

        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Starburst Enterprise agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        connect_args: Dict[str, Any] = {**credentials[_ATTR_CONNECT_ARGS]}

        # Store connection info for HTTP requests (before they get modified)
        self._host: str = connect_args.get("host", "")
        self._port: str = str(connect_args.get("port", "443"))
        self._user: str = connect_args.get("user", "")
        self._password: str = connect_args.get("password", "")

        if not self._host:
            raise ValueError(
                "Starburst Enterprise agent client requires 'host' in connect_args"
            )

        # Handle SSL options - used for both Trino and HTTP requests
        ssl_options = SslOptions(**(connect_args.pop("ssl_options", {}) or {}))
        self._ssl_verify: Union[bool, str, None] = None

        if ssl_options.ca_data and not ssl_options.disabled:
            host_hash = hashlib.sha256(self._host.encode()).hexdigest()[:12]
            cert_file = f"/tmp/{host_hash}_starburst_enterprise_ca.pem"
            ssl_options.write_ca_data_to_temp_file(cert_file, upsert=True)
            self._ssl_verify = cert_file
            logger.info("Starburst Enterprise SSL configured")

        if ssl_options.disabled:
            self._ssl_verify = False

        connect_args["verify"] = self._ssl_verify

        # Setup Trino connection (same as base StarburstProxyClient)
        if "user" not in connect_args or "password" not in connect_args:
            raise ValueError(
                "Starburst Enterprise agent client requires 'user' and 'password' in connect_args"
            )
        user = connect_args.pop("user")
        password = connect_args.pop("password")
        connect_args["auth"] = trino.auth.BasicAuthentication(user, password)

        self._connection = trino.dbapi.connect(**connect_args)

        # Setup HTTP client for REST API calls using Basic Auth
        # No ssl_options needed - we pass verify_ssl at request time using the same value as Trino
        basic_token = base64.b64encode(
            f"{self._user}:{self._password}".encode("utf-8")
        ).decode("ascii")
        http_credentials: Dict[str, Any] = {
            "token": basic_token,
            "auth_type": "Basic",
        }
        self._http_client = HttpProxyClient(credentials=http_credentials)

    @property
    def wrapped_client(self):
        return self._connection

    def _get_rest_base_url(self) -> str:
        """Returns the base URL for REST API calls using credentials."""
        return f"https://{self._host}:{self._port}"

    def _build_url(self, path: str) -> str:
        """Builds a full URL from a path."""
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{self._get_rest_base_url()}{path}"

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
        Execute an HTTP request to the Starburst Enterprise host using credentials.

        :param path: The path to append to the base URL (e.g., "/v1/cluster")
        :param http_method: HTTP method (GET, POST, PUT, DELETE, etc.)
        :param payload: Optional JSON payload for the request body
        :param content_type: Optional Content-Type header value
        :param timeout: Optional timeout in seconds
        :param additional_headers: Optional additional headers
        :param params: Optional query string parameters
        :param retry_status_code_ranges: Optional status codes that trigger retry
        :param data: Optional raw data for the request body
        :return: JSON response (dict or list) or empty dict for empty responses
        """
        url = self._build_url(path)

        # Add Accept header for JSON responses
        headers = {"Accept": "application/json"}
        if additional_headers:
            headers.update(additional_headers)

        return self._http_client.do_request(
            url=url,
            http_method=http_method,
            payload=payload,
            content_type=content_type,
            timeout=timeout,
            additional_headers=headers,
            params=params,
            verify_ssl=self._ssl_verify,
            retry_status_code_ranges=retry_status_code_ranges,
            data=data,
        )

    # -------------------------------------------------------------------------
    # Error handling - delegate to HttpProxyClient for HTTP errors
    # -------------------------------------------------------------------------

    def get_error_type(self, error: Exception) -> Optional[str]:
        # Delegate HTTP error handling to the http client
        http_error_type = self._http_client.get_error_type(error)
        if http_error_type:
            return http_error_type
        return super().get_error_type(error=error)

    def get_error_extra_attributes(self, error: Exception) -> Optional[Dict]:
        # Delegate HTTP error handling to the http client
        http_attrs = self._http_client.get_error_extra_attributes(error)
        if http_attrs:
            return http_attrs
        return super().get_error_extra_attributes(error=error)

