import hashlib
import logging
from typing import Dict, Optional, List, Tuple, Union

import requests
from requests import HTTPError
from retry.api import retry_call

from apollo.common.agent.models import AgentOperation
from apollo.common.agent.redact import AgentRedactUtilities
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.db.base_db_proxy_client import SslOptions


_logger = logging.getLogger(__name__)

_DEFAULT_RETRY_STATUS_CODE_RANGES = [
    (429, 430),
    (500, 600),
]
_HTTP_REDACTED_ATTRIBUTES = [
    "payload",
    "data",
    "params",
]

_RRI = dict(
    tries=2,
    delay=2,
    backoff=2,
    max_delay=10,
)

_ATTR_CONNECT_ARGS = "connect_args"


class HttpClientError(Exception):
    pass


class HttpRetryableError(Exception):
    pass


class HttpProxyClient(BaseProxyClient):
    """
    Proxy client class to perform HTTP requests from the agent.
    It supports simple no-retry requests and requests with retries for a subset of status codes.
    SSL options can be configured via credentials using the `ssl_options` key, supporting:
    - `ca_data`: CA certificate data for SSL verification
    - `disabled`: Set to True to disable SSL verification
    """

    def __init__(self, credentials: Optional[Dict], **kwargs):  # type: ignore
        self._credentials = credentials
        self._ssl_verify: Union[bool, str, None] = None

        if credentials:
            connect_args: Dict[str, Any] = {**credentials.get(_ATTR_CONNECT_ARGS, {})}

            # Handle SSL options from credentials
            if connect_args:
                ssl_options = SslOptions(**(connect_args.get("ssl_options", {}) or {}))

                if ssl_options.ca_data and not ssl_options.disabled:
                    # requests library accepts a path to a CA bundle file for verification
                    # Create a temporary file for the CA certificate
                    # Use a hash of the ca_data to create a unique filename
                    ca_hash = hashlib.sha256(ssl_options.ca_data.encode()).hexdigest()[
                        :12
                    ]
                    cert_file = f"/tmp/{ca_hash}_http_ca.pem"
                    ssl_options.write_ca_data_to_temp_file(cert_file, upsert=True)

                    self._ssl_verify = cert_file
                    _logger.debug("HTTP SSL configured with custom CA certificate")

                if ssl_options.disabled:
                    self._ssl_verify = False
                    _logger.debug("HTTP SSL verification disabled")

    @property
    def wrapped_client(self):
        return None

    @staticmethod
    def is_client_error_status_code(status_code: int) -> bool:
        return 400 <= status_code < 500

    def log_payload(self, operation: AgentOperation) -> Dict:
        """
        Implements `log_payload` from `BaseProxyClient` to additionally redact
        "payload" and "data" attributes, preventing OAuth tokens from being logged.
        """
        payload = super().log_payload(operation)
        return AgentRedactUtilities.redact_attributes(
            payload, _HTTP_REDACTED_ATTRIBUTES
        )

    def do_request(
        self,
        url: str,
        http_method: str = "POST",
        payload: Optional[Dict] = None,
        content_type: Optional[str] = None,
        timeout: Optional[int] = None,
        user_agent: Optional[str] = None,
        additional_headers: Optional[Dict] = None,
        params: Optional[Dict] = None,
        verify_ssl: Optional[bool] = None,
        retry_status_code_ranges: Optional[List[Tuple]] = None,
        data: Optional[str] = None,
    ) -> Dict:
        """
        Executes a single request with no retry, intended to be used for JSON request/response endpoints.
        If the status code is included in `retry_status_code_ranges` then `HttpRetryableError` will be raised.
        Throws HTTPError by calling response.raise_for_status internally.
        :param url: required URL for the request
        :param http_method: HTTP method for the request, defaults to POST
        :param payload: optional JSON payload
        :param content_type: optional value for Content-Type header
        :param timeout: optional timeout in seconds
        :param user_agent: optional value for User-Agent header
        :param additional_headers: optional headers
        :param params: optional parameters dictionary to include in the query string.
        :param verify_ssl: optional boolean which controls whether we verify the server's TLS certificate.
            Takes precedence over ssl_options configured in credentials.
        :param retry_status_code_ranges: optional list of ranges specifying status code to raise `HttpRetryableError`.
            The ranges are expected to be specified in a list of tuples where each tuple includes two elements:
            inclusive from and exclusive to, for example: [(500, 600)] means: `500 <= status_code < 600`.
        :return: the JSON result of the request
        """

        request_args = {}
        if payload:
            request_args["json"] = payload
        if data:
            request_args["data"] = data
        if timeout:
            request_args["timeout"] = timeout
        if params:
            request_args["params"] = params
        if verify_ssl is not None:
            request_args["verify"] = verify_ssl
        elif self._ssl_verify is not None:
            request_args["verify"] = self._ssl_verify

        headers = {**additional_headers} if additional_headers else {}
        if self._credentials and "token" in self._credentials:
            auth_header = self._credentials.get("auth_header", "Authorization")
            auth_header_value = self._credentials["token"]
            if auth_type := self._credentials.get("auth_type", "Bearer"):
                auth_header_value = f"{auth_type} {auth_header_value}"
            headers[auth_header] = auth_header_value
        if content_type:
            headers["Content-Type"] = content_type
        if user_agent:
            headers["User-Agent"] = user_agent
        request_args["headers"] = headers

        response = requests.request(http_method, url, **request_args)
        try:
            response.raise_for_status()
        except HTTPError as err:
            status_code = response.status_code
            text = response.text or str(err)
            _logger.exception(
                f"Request failed with {status_code}",
                extra=dict(error_text=text),
            )
            if retry_status_code_ranges is not None and self._is_retry_status_code(
                retry_status_code_ranges, status_code
            ):
                # retry for this status code
                raise HttpRetryableError(text) from err
            if self.is_client_error_status_code(status_code):
                raise HttpClientError(text) from err
            raise type(err)(text) from err

        return response.json()

    def do_request_with_retry(
        self,
        url: str,
        http_method: str = "POST",
        payload: Optional[Dict] = None,
        content_type: Optional[str] = None,
        timeout: Optional[int] = None,
        user_agent: Optional[str] = None,
        additional_headers: Optional[Dict] = None,
        params: Optional[Dict] = None,
        retry_status_code_ranges: Optional[List[Tuple]] = None,
        retry_args: Optional[Dict] = None,
    ) -> Dict:
        """
        Same as `do_request` but retrying based on the status codes defined by `retry_status_code_ranges` and
        `retry_args`.
        `retry_status_code_args` defaults to 429 and 5xx errors
        `retry_args` defaults to: tries=2, delay=2, backoff=2, max_delay=10
        """

        retry_status_code_ranges = (
            retry_status_code_ranges or _DEFAULT_RETRY_STATUS_CODE_RANGES
        )
        retry_params = retry_args or _RRI
        return retry_call(
            self.do_request,
            fkwargs={
                "url": url,
                "http_method": http_method,
                "payload": payload,
                "content_type": content_type,
                "timeout": timeout,
                "user_agent": user_agent,
                "additional_headers": additional_headers,
                "params": params,
                "retry_status_code_ranges": retry_status_code_ranges,
            },
            exceptions=HttpRetryableError,
            **retry_params,  # type: ignore
        )

    def get_error_type(self, error: Exception) -> Optional[str]:
        cause = error.__cause__ or error
        if isinstance(cause, HTTPError):
            return "HTTPError"
        else:
            return super().get_error_type(error=error)

    def get_error_extra_attributes(self, error: Exception) -> Optional[Dict]:
        cause = error.__cause__ or error
        if isinstance(cause, HTTPError) and cause.response is not None:
            return {
                "status_code": cause.response.status_code,
                "reason": cause.response.reason,
            }
        else:
            return super().get_error_extra_attributes(error=error)

    @staticmethod
    def _is_retry_status_code(ranges: List[Tuple], status_code: int) -> bool:
        return any(r for r in ranges if r[0] <= status_code < r[1])
