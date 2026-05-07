import ipaddress
import logging
import os
import tempfile
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urlsplit

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

    def __init__(
        self,
        credentials: Optional[Dict],
        platform: Optional[str] = None,
        **kwargs,  # type: ignore
    ):
        self._ssl_verify: Union[bool, str, None] = None
        # `platform` is forwarded to the storage factory by `download_to_storage`
        # so the configured backend (S3/GCS/Azure) can be derived from the agent
        # platform when `MCD_STORAGE` is unset (the production default — the env
        # var is only set for local development). Defaults to None so direct
        # callers (e.g. DatabricksRestProxyClient) that don't use
        # download_to_storage keep working.
        self._platform: Optional[str] = platform

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

    @staticmethod
    def _assert_safe_download_url(url: str) -> None:
        # Phrasing is method-agnostic so the same helper can be reused by every
        # streaming download entry point on this client (download_bytes,
        # download_to_storage, future additions) without misnaming the caller.
        parts = urlsplit(url)
        if parts.scheme != "https":
            raise HttpClientError(
                f"download requires https scheme; got '{parts.scheme}'"
            )
        host = (parts.hostname or "").lower()
        if host in ("", "localhost"):
            raise HttpClientError(f"download refuses '{host or '<empty>'}' host")
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            return  # not a literal IP — DNS hostname is OK
        # Reject every IP-literal class that is not appropriate as a download
        # target. `is_global` is False for private (RFC1918), loopback,
        # link-local, reserved, and unspecified (0.0.0.0 / ::) — but Python's
        # ipaddress module reports `is_global=True` for multicast (e.g.
        # 224.0.0.0/4), so we add an explicit multicast check.
        if not ip.is_global or ip.is_multicast:
            raise HttpClientError(f"download refuses non-public address: {ip}")

    def _attach_auth_header(self, headers: Dict) -> None:
        if self._credentials and "token" in self._credentials:
            auth_header = self._credentials.get("auth_header", "Authorization")
            auth_value = self._credentials["token"]
            if auth_type := self._credentials.get("auth_type", "Bearer"):
                auth_value = f"{auth_type} {auth_value}"
            headers[auth_header] = auth_value

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

        URL validation: ``do_request`` does NOT validate the destination URL — no
        scheme check, no host allowlist, no IP-range guard. The agent trusts the
        caller (typically the Monte Carlo DC) for URL construction. This is a
        deliberate design choice for the MuleSoft and ``http`` connection types,
        where the DC owns endpoint selection. Callers that need SSRF defenses
        (e.g., for downloading from caller-supplied URLs that may originate further
        upstream) should use ``download_bytes`` instead, which calls
        ``_assert_safe_download_url``.

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
        self._attach_auth_header(headers)
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

    def download_bytes(
        self,
        url: str,
        timeout: int = 120,
        max_bytes: Optional[int] = None,
        no_auth: bool = True,
        additional_headers: Optional[Dict] = None,
    ) -> bytes:
        """Generic streaming GET for binary payloads. Use ``no_auth=True`` (the
        default) for pre-signed or otherwise credential-free URLs.

        - ``no_auth=False`` attaches the Bearer token from ``connect_args``;
          defaults to True to avoid leaking auth to unintended hosts.
        - ``max_bytes`` enforces a size limit during chunked read (defense
          against memory exhaustion from a malicious or buggy URL).
        - ``additional_headers`` are merged into the request headers (auth, if
          enabled, is attached on top).
        - 4xx → ``HttpClientError``; 5xx → ``HTTPError`` (matches do_request).
        - The URL is rejected if it is not https or resolves to a non-public
          IP literal — defense-in-depth against SSRF.
        - Redirects are NOT followed: a 30x response is treated as an
          ``HttpClientError``. Re-following a redirect would bypass the
          URL safety guard (allowing SSRF to internal/non-https targets) and,
          when ``no_auth=False``, could forward credentials to an unintended
          host. Pre-signed URLs (the motivating use case) resolve directly
          to bytes — an unexpected redirect indicates a misconfiguration.
        - SSL verification follows ``self._ssl_verify``.
        - Error messages do not include the URL — pre-signed URLs contain a
          signed secret; callers needing verbose error context should use
          ``do_request`` with full URLs instead.
        """
        self._assert_safe_download_url(url)

        headers = {**additional_headers} if additional_headers else {}
        if not no_auth:
            self._attach_auth_header(headers)

        request_kwargs: Dict = {
            "timeout": timeout,
            "headers": headers,
            "stream": True,
            "allow_redirects": False,
        }
        if self._ssl_verify is not None:
            request_kwargs["verify"] = self._ssl_verify

        try:
            response = requests.get(url, **request_kwargs)
        except requests.RequestException as exc:
            raise HttpClientError(
                f"download_bytes transport error: {type(exc).__name__}"
            ) from None

        with response:
            status = response.status_code
            # 3xx is not raised by raise_for_status; explicitly reject it so
            # callers don't silently receive an empty redirect body as "bytes".
            if 300 <= status < 400:
                raise HttpClientError(
                    f"download_bytes refused redirect (HTTP {status})"
                )
            try:
                response.raise_for_status()
            except HTTPError:
                if self.is_client_error_status_code(status):
                    raise HttpClientError(
                        f"download_bytes failed with HTTP {status}"
                    ) from None
                new_err = HTTPError(f"download_bytes failed with HTTP {status}")
                new_err.response = response
                raise new_err from None
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

    def download_to_storage(
        self,
        url: str,
        storage_key: str,
        timeout: int = 300,
        max_bytes: Optional[int] = None,
        no_auth: bool = True,
        additional_headers: Optional[Dict] = None,
    ) -> str:
        """Stream a binary download into the agent's configured storage backend
        (S3 / GCS / Azure Blob / S3-compatible) without holding the payload in
        memory — only one chunk (8 KiB) is in memory at any time.

        Bytes are spooled to a tempfile as they arrive, then handed to the
        storage backend's ``upload_file``; the tempfile is deleted in the
        ``finally``. Returns the supplied ``storage_key``.

        Same safety surface as ``download_bytes``: HTTPS-only, non-public IP
        literals (incl. multicast) rejected, redirects refused, ``max_bytes``
        cap fires mid-stream, error messages strip the URL, connection released
        on every exit path.

        Use this instead of ``download_bytes`` for payloads that may be large
        enough to matter for memory (the motivating case is MuleSoft Mule
        application JARs which can exceed 100 MiB).
        """
        self._assert_safe_download_url(url)

        headers = {**additional_headers} if additional_headers else {}
        if not no_auth:
            self._attach_auth_header(headers)

        request_kwargs: Dict = {
            "timeout": timeout,
            "headers": headers,
            "stream": True,
            "allow_redirects": False,
        }
        if self._ssl_verify is not None:
            request_kwargs["verify"] = self._ssl_verify

        try:
            response = requests.get(url, **request_kwargs)
        except requests.RequestException as exc:
            raise HttpClientError(
                f"download_to_storage transport error: {type(exc).__name__}"
            ) from None

        # Late import: storage layer pulls in cloud SDKs that we don't want at
        # HttpProxyClient construction time (this method is the only consumer).
        from apollo.integrations.storage.factory import get_storage_client

        with response:
            status = response.status_code
            if 300 <= status < 400:
                raise HttpClientError(
                    f"download_to_storage refused redirect (HTTP {status})"
                )
            try:
                response.raise_for_status()
            except HTTPError:
                if self.is_client_error_status_code(status):
                    raise HttpClientError(
                        f"download_to_storage failed with HTTP {status}"
                    ) from None
                new_err = HTTPError(f"download_to_storage failed with HTTP {status}")
                new_err.response = response
                raise new_err from None

            tmp = tempfile.NamedTemporaryFile(
                delete=False, suffix=".download_to_storage"
            )
            try:
                size = 0
                try:
                    for chunk in response.iter_content(chunk_size=8192):
                        if not chunk:
                            continue
                        tmp.write(chunk)
                        size += len(chunk)
                        if max_bytes is not None and size > max_bytes:
                            raise HttpClientError(
                                f"download_to_storage response exceeded "
                                f"{max_bytes} bytes"
                            )
                finally:
                    tmp.close()
                # Forward the agent platform so the storage factory can fall back
                # to the platform default (S3/GCS/Azure) when MCD_STORAGE is unset.
                get_storage_client(platform=self._platform).upload_file(
                    storage_key, tmp.name
                )
            finally:
                # Best-effort cleanup; never let an unlink failure mask the
                # original exception (or stomp on a successful return).
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass

        return storage_key

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
