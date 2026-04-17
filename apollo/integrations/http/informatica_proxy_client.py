import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.http.http_proxy_client import HttpProxyClient

logger = logging.getLogger(__name__)

_ATTR_CONNECT_ARGS = "connect_args"

_DEFAULT_BASE_URL = "https://dm-us.informaticacloud.com"
_DEFAULT_AUTH_VERSION = "v3"

_V2_LOGIN_PATH = "/ma/api/v2/user/login"
_V3_LOGIN_PATH = "/saas/public/core/v3/login"

_INTEGRATION_CLOUD_PRODUCT_NAME = "Integration Cloud"


class InformaticaLoginError(Exception):
    pass


class InformaticaProxyClient(BaseProxyClient):
    """
    Proxy client for Informatica Cloud connections.

    Performs V2 or V3 login in __init__ using form-encoded credentials, then exposes
    do_http_request() for API calls authenticated with the session ID returned by login.

    Expected credentials shape:
        credentials["connect_args"]:
            username          (required)
            password          (required)
            informatica_auth  (optional, "v2" or "v3", default "v3")
            base_url          (optional login base URL, default "https://dm-us.informaticacloud.com")
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Informatica agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )

        connect_args: Dict[str, Any] = credentials[_ATTR_CONNECT_ARGS]

        username = connect_args.get("username")
        password = connect_args.get("password")
        if not username or not password:
            raise ValueError(
                "Informatica agent client requires 'username' and 'password' in connect_args"
            )

        auth_version: str = connect_args.get("informatica_auth", _DEFAULT_AUTH_VERSION)
        login_base_url: str = connect_args.get("base_url", _DEFAULT_BASE_URL).rstrip(
            "/"
        )

        if auth_version == "v2":
            self._api_base_url, session_id = self._login_v2(
                login_base_url, username, password
            )
        else:
            self._api_base_url, session_id = self._login_v3(
                login_base_url, username, password
            )

        # icSessionId is used for all API calls regardless of auth version — this is coupled
        # to V2 API usage, not the auth version.
        http_credentials: Dict[str, Any] = {
            "token": session_id,
            "auth_header": "icSessionId",
            "auth_type": "",  # empty string → send token as-is, no "Bearer " prefix
        }
        self._http_client = HttpProxyClient(credentials=http_credentials)

    @property
    def wrapped_client(self):
        return None

    # -------------------------------------------------------------------------
    # Login helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _login_v2(login_base_url: str, username: str, password: str) -> Tuple[str, str]:
        """
        Performs V2 login and returns (api_base_url, session_id).
        Raises InformaticaLoginError on failure.
        """
        url = f"{login_base_url}{_V2_LOGIN_PATH}"
        response = requests.post(url, data={"username": username, "password": password})
        try:
            response.raise_for_status()
            body = response.json()
            server_url = body.get("serverUrl")
            session_id = body.get("icSessionId")
            if not server_url or not session_id:
                raise InformaticaLoginError(
                    "Informatica V2 login failed: response did not contain expected "
                    "serverUrl or icSessionId"
                )
            return server_url, session_id
        except InformaticaLoginError:
            raise
        except Exception as exc:
            raise InformaticaLoginError(
                "Informatica V2 login failed: unexpected error during login"
            ) from exc

    @staticmethod
    def _login_v3(login_base_url: str, username: str, password: str) -> Tuple[str, str]:
        """
        Performs V3 login and returns (api_base_url, session_id).
        Raises InformaticaLoginError on failure.
        """
        url = f"{login_base_url}{_V3_LOGIN_PATH}"
        response = requests.post(url, data={"username": username, "password": password})
        try:
            response.raise_for_status()
            body = response.json()

            products = body.get("products") or []
            api_base_url = next(
                (
                    p.get("baseApiUrl")
                    for p in products
                    if p.get("name") == _INTEGRATION_CLOUD_PRODUCT_NAME
                ),
                None,
            )
            user_info = body.get("userInfo") or {}
            session_id = user_info.get("sessionId")

            if not api_base_url or not session_id:
                raise InformaticaLoginError(
                    "Informatica V3 login failed: response did not contain expected "
                    "Integration Cloud baseApiUrl or userInfo.sessionId"
                )
            return api_base_url, session_id
        except InformaticaLoginError:
            raise
        except Exception as exc:
            raise InformaticaLoginError(
                "Informatica V3 login failed: unexpected error during login"
            ) from exc

    # -------------------------------------------------------------------------
    # HTTP request
    # -------------------------------------------------------------------------

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

    # -------------------------------------------------------------------------
    # Error handling - delegate to HttpProxyClient for HTTP errors
    # -------------------------------------------------------------------------

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
