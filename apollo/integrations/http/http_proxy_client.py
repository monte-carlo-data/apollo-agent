import logging
from typing import Dict, Optional, List, Tuple, Union

import requests
from requests import HTTPError
from retry.api import retry_call

from apollo.common.agent.models import AgentOperation
from apollo.common.agent.redact import AgentRedactUtilities
from apollo.integrations.base_proxy_client import BaseProxyClient

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
        self._ssl_verify: Union[bool, str, None] = None

        if credentials and "connect_args" in credentials:
            self._credentials = credentials["connect_args"]
            ssl_verify = self._credentials.get("ssl_verify")
            if ssl_verify is not None:
                self._ssl_verify = ssl_verify
        else:
            # Used when HttpProxyClient is instantiated directly (e.g. by other proxy clients)
            self._credentials = credentials

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
        verify_ssl: Optional[Union[bool, str]] = None,
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

    def do_request_relative(
        self,
        path: str,
        http_method: str = "POST",
        payload: Optional[Dict] = None,
        content_type: Optional[str] = None,
        timeout: Optional[int] = None,
        user_agent: Optional[str] = None,
        additional_headers: Optional[Dict] = None,
        params: Optional[Dict] = None,
        verify_ssl: Optional[Union[bool, str]] = None,
        retry_status_code_ranges: Optional[List[Tuple]] = None,
        data: Optional[str] = None,
    ) -> Dict:
        """Like ``do_request`` but treats ``path`` as a path on a base URL stored
        in ``self._credentials["api_base_url"]``. The CTP for the connection_type
        populates api_base_url; the caller (DC) supplies arbitrary endpoint paths
        without knowing the base URL — adding a new endpoint requires no agent
        release.
        """
        if not self._credentials or "api_base_url" not in self._credentials:
            raise HttpClientError(
                "do_request_relative requires 'api_base_url' in connect_args"
            )
        base = self._credentials["api_base_url"].rstrip("/")
        if not path.startswith("/"):
            path = "/" + path
        return self.do_request(
            url=f"{base}{path}",
            http_method=http_method,
            payload=payload,
            content_type=content_type,
            timeout=timeout,
            user_agent=user_agent,
            additional_headers=additional_headers,
            params=params,
            verify_ssl=verify_ssl,
            retry_status_code_ranges=retry_status_code_ranges,
            data=data,
        )

    def download_bytes(
        self,
        url: str,
        timeout: int = 120,
        max_bytes: Optional[int] = None,
        no_auth: bool = False,
        additional_headers: Optional[Dict] = None,
    ) -> bytes:
        """Download raw bytes via streaming GET. Generic helper for binary
        payloads (e.g. JAR/ZIP fetches from S3 pre-signed URLs).

        - ``no_auth=True`` skips the Authorization header (required for S3
          pre-signed URLs which carry the signature in the query string).
        - ``max_bytes`` enforces a size limit during chunked read (defense
          against memory exhaustion).
        - 4xx → ``HttpClientError``; 5xx → ``HTTPError`` (matches do_request).
        - SSL verification follows ``self._ssl_verify``.
        - Error messages do not include the URL — pre-signed URLs contain a
          signed secret; callers needing verbose error context should use
          ``do_request`` with full URLs instead.
        """
        headers = {**additional_headers} if additional_headers else {}
        if not no_auth and self._credentials and "token" in self._credentials:
            auth_header = self._credentials.get("auth_header", "Authorization")
            auth_value = self._credentials["token"]
            if auth_type := self._credentials.get("auth_type", "Bearer"):
                auth_value = f"{auth_type} {auth_value}"
            headers[auth_header] = auth_value

        request_kwargs: Dict = {"timeout": timeout, "headers": headers, "stream": True}
        if self._ssl_verify is not None:
            request_kwargs["verify"] = self._ssl_verify

        try:
            response = requests.get(url, **request_kwargs)
        except requests.RequestException as exc:
            raise HttpClientError(
                f"download_bytes transport error: {type(exc).__name__}"
            ) from None

        try:
            response.raise_for_status()
        except HTTPError:
            status = response.status_code
            if self.is_client_error_status_code(status):
                raise HttpClientError(
                    f"download_bytes failed with HTTP {status}"
                ) from None
            raise HTTPError(f"download_bytes failed with HTTP {status}") from None

        chunks: List[bytes] = []
        size = 0
        for chunk in response.iter_content(chunk_size=8192):
            if not chunk:
                continue
            chunks.append(chunk)
            size += len(chunk)
            if max_bytes is not None and size > max_bytes:
                raise HttpClientError(
                    f"download_bytes response exceeded {max_bytes} bytes"
                )
        return b"".join(chunks)

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
