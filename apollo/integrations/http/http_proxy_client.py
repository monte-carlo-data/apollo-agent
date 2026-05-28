import logging
import os
import tempfile
from contextlib import contextmanager
from typing import Dict, Iterator, List, Optional, Tuple, Union

import requests
from requests import HTTPError
from retry.api import retry_call

from apollo.common.agent.models import AgentOperation
from apollo.common.agent.redact import AgentRedactUtilities
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.http.url_safety import (
    HttpClientError as HttpClientError,
    safe_request,
    safety_policy,
)
from apollo.integrations.storage.base_storage_client import BaseStorageClient

# Hoisted to module scope: a late import inside `download_to_storage` would
# leak the streaming response if the import raised (e.g., missing cloud SDK).
# Safe at module scope because `factory.py`'s SDK imports are themselves lazy.
from apollo.integrations.storage.factory import get_storage_client

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
        # Lazy-initialized on first download_to_storage call; reused across
        # subsequent calls so the underlying SDK client (boto3 / google-cloud-
        # storage / azure-storage-blob) is constructed once per HttpProxyClient
        # instance instead of per request.
        self._storage_client: Optional[BaseStorageClient] = None

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

    def _get_storage_client(self) -> BaseStorageClient:
        """Lazy + cached storage client for ``download_to_storage``.

        Constructed once per ``HttpProxyClient`` instance — reusing the
        underlying SDK client (boto3 / google-cloud-storage / azure-storage-
        blob) across multiple ``download_to_storage`` calls avoids the per-call
        cost of credential resolution and connection-pool setup.
        """
        if self._storage_client is None:
            self._storage_client = get_storage_client(platform=self._platform)
        return self._storage_client

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

        URL validation: the destination URL is validated against the SSRF block
        list in ``apollo.integrations.http.url_safety`` (default-blocks cloud
        metadata services and loopback; operator-extensible via
        ``MCD_HTTP_BLOCKED_CIDRS``) and the underlying TCP connect is pinned to
        the validated IP to close DNS-rebinding. RFC1918 is intentionally
        allowed — VPC/Private-Link traffic is in scope for this client.
        Callers needing the stricter "public-only" policy (e.g. for
        caller-supplied URLs whose host comes from further upstream) should
        use ``download_bytes`` instead, which wraps the request in
        ``safety_policy(url, strict_ip_policy=True, https_only=True)``.

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

        response = safe_request(http_method, url, **request_args)
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

    @contextmanager
    def _open_download_response(
        self,
        url: str,
        timeout: int,
        no_auth: bool,
        additional_headers: Optional[Dict],
        op_label: str,
        method: str = "GET",
    ) -> Iterator[requests.Response]:
        """Open a streaming response with the full SSRF + redirect + status guards.

        Use as a context manager: ``with self._open_download_response(...) as response:``.
        On every error-exit path the response is closed internally before the
        exception propagates; on success, the context manager closes it on exit.
        ``op_label`` is included in error messages so the user-facing message
        names the calling method.

        Defaults to ``method="GET"`` for backwards compatibility; pass
        ``method="HEAD"`` to issue a HEAD request instead (used by
        ``head_bytes`` to do ETag / Last-Modified change-detection against
        pre-signed URLs without downloading the payload).

        Raises:
            HttpClientError: SSRF guard rejection, transport error, 3xx redirect,
                or 4xx response.
            HTTPError: 5xx response.
        """
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

        # Dispatch on method name rather than `requests.request(method, ...)`
        # so existing tests that patch ``requests.get`` continue to intercept
        # GETs unchanged. ``requests.head`` and ``requests.get`` have the same
        # kwargs surface.
        if method == "HEAD":
            request_func = requests.head
        else:
            request_func = requests.get
        # Strict tier: downloads only ever target signed public HTTPS URLs;
        # non-public IPs and non-HTTPS schemes are out-of-band. ``allow_redirects=False``
        # is set in ``request_kwargs`` above, so the IP guard installed by
        # ``safety_policy`` only runs for the initial connection.
        try:
            with safety_policy(url, strict_ip_policy=True, https_only=True):
                response = request_func(url, **request_kwargs)
        except requests.RequestException as exc:
            raise HttpClientError(
                f"{op_label} transport error: {type(exc).__name__}"
            ) from None

        status = response.status_code
        if 300 <= status < 400:
            response.close()
            raise HttpClientError(f"{op_label} refused redirect (HTTP {status})")
        try:
            response.raise_for_status()
        except HTTPError:
            if self.is_client_error_status_code(status):
                response.close()
                raise HttpClientError(f"{op_label} failed with HTTP {status}") from None
            new_err = HTTPError(f"{op_label} failed with HTTP {status}")
            new_err.response = response
            response.close()
            raise new_err from None

        try:
            yield response
        finally:
            response.close()

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
        with self._open_download_response(
            url,
            timeout=timeout,
            no_auth=no_auth,
            additional_headers=additional_headers,
            op_label="download_bytes",
        ) as response:
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

    def head_bytes(
        self,
        url: str,
        timeout: int = 60,
        no_auth: bool = True,
        additional_headers: Optional[Dict] = None,
    ) -> Dict[str, str]:
        """Issue a HEAD request and return the response headers as a plain dict.

        The motivating use case is cheap change-detection against pre-signed
        URLs: callers compare an upstream ``ETag`` / ``Last-Modified`` header
        against a cached value and skip a multi-MB download when unchanged.

        Same safety surface as ``download_bytes``: HTTPS-only, non-public IP
        literals (incl. multicast) rejected, redirects refused, error messages
        strip the URL, connection released on every exit path. ``no_auth``
        defaults to True for parity with ``download_bytes`` — the motivating
        use case is pre-signed URLs that already carry their auth.

        Returns:
            A plain ``dict`` of header name → value. Header names follow the
            case the upstream server sent; callers should match
            case-insensitively. JSON-serializable so the value survives the
            ``@agent_operation`` round-trip back to data-collector.

        Raises:
            HttpClientError: SSRF guard rejection, transport error,
                or 3xx/4xx response.
            HTTPError: 5xx response.
        """
        with self._open_download_response(
            url,
            timeout=timeout,
            no_auth=no_auth,
            additional_headers=additional_headers,
            op_label="head_bytes",
            method="HEAD",
        ) as response:
            return dict(response.headers)

    def probe_response_headers(
        self,
        url: str,
        *,
        range_spec: str = "bytes=0-0",
        timeout: int = 60,
        no_auth: bool = True,
        additional_headers: Optional[Dict] = None,
    ) -> Dict[str, str]:
        """Issue a GET with a ``Range`` header and return only the response
        headers as a plain dict. The body is discarded on response close.

        Designed as a sibling to ``head_bytes`` for endpoints where HEAD is
        forbidden. The motivating case is AWS pre-signed URLs, whose
        signatures are method-bound — a URL signed for GET returns 403 on
        HEAD. A Range-GET sidesteps that: the signature matches (still a
        GET) and the response carries the headers the caller cares about
        (``ETag`` / ``Last-Modified`` / ``Content-Length``) plus a tiny
        bounded body that the connection-close throws away.

        ``range_spec`` defaults to ``bytes=0-0`` (1 byte). Hosts that
        honour Range respond ``206 Partial Content``; hosts that don't
        respond ``200 OK`` with the full body — the connection close
        still discards it without ever reading more than the headers.
        Callers can override ``range_spec`` for use cases like format
        sniffing (``bytes=0-4095``).

        ``additional_headers`` win on key collision with the implicit
        ``Range`` header, so a caller passing their own ``Range`` overrides
        ``range_spec``. This matters because the caller may want a
        different byte range than the default for the same probe.

        Same safety surface as ``head_bytes`` / ``download_bytes``:
        HTTPS-only, non-public IP literals rejected, redirects refused,
        error messages strip the URL, connection released on every exit
        path. ``no_auth`` defaults to True for parity — the motivating
        use case is pre-signed URLs that already carry their auth.

        Returns:
            A plain ``dict`` of header name → value. Header names follow
            the case the upstream server sent; callers should match
            case-insensitively. JSON-serializable so the value survives
            the ``@agent_operation`` round-trip back to data-collector.

        Raises:
            HttpClientError: SSRF guard rejection, transport error,
                or 3xx/4xx response.
            HTTPError: 5xx response.
        """
        # Implicit Range goes first so a caller-supplied Range in
        # additional_headers wins via dict-merge order.
        merged_headers: Dict = {"Range": range_spec}
        if additional_headers:
            merged_headers.update(additional_headers)
        with self._open_download_response(
            url,
            timeout=timeout,
            no_auth=no_auth,
            additional_headers=merged_headers,
            op_label="probe_response_headers",
            method="GET",
        ) as response:
            return dict(response.headers)

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

        Notes:
            Requires either ``MCD_STORAGE`` env var, or a ``platform`` value passed
            to ``HttpProxyClient(...)`` whose default backend is recognized
            (`PLATFORM_AWS`, `PLATFORM_GCP`, `PLATFORM_AZURE`, `PLATFORM_AWS_GENERIC`).

        Raises:
            HttpClientError: SSRF guard rejection, transport error, 3xx/4xx response,
                or ``max_bytes`` exceeded.
            HTTPError: 5xx response.
            AgentConfigurationError: storage backend is not configured (no
                ``MCD_STORAGE`` env var and no recognized ``platform``).
        """
        with self._stream_to_tempfile(
            url,
            timeout=timeout,
            max_bytes=max_bytes,
            no_auth=no_auth,
            additional_headers=additional_headers,
            op_label="download_to_storage",
        ) as tmp_path:
            # Cached on self after first call — avoids reconstructing the
            # SDK client (boto3 / GCS / Azure) per request.
            self._get_storage_client().upload_file(storage_key, tmp_path)

        return storage_key

    @contextmanager
    def _stream_to_tempfile(
        self,
        url: str,
        *,
        op_label: str,
        timeout: int,
        max_bytes: Optional[int],
        no_auth: bool,
        additional_headers: Optional[Dict],
    ) -> Iterator[str]:
        """Stream a download via ``_open_download_response`` into a transient
        tempfile, yield the tempfile path to the caller, and delete the
        tempfile on every exit path (success and every failure).

        Shared between ``download_to_storage`` and the MuleSoft subclass's
        ``extract_mulesoft_sources`` so the streaming-download semantics
        (8 KiB chunked read, ``max_bytes`` cap mid-stream, ``finally:
        os.unlink``) live in exactly one place. New per-op variations of
        the pattern should also use this helper rather than re-implement
        it inline.

        ``op_label`` is woven through to ``_open_download_response`` (for
        the error-message prefix) and into the tempfile suffix (for
        ``ls /tmp`` triage).

        Raises:
            HttpClientError: SSRF guard rejection, transport error, 3xx/4xx
                response, or ``max_bytes`` exceeded.
            HTTPError: 5xx response.
        """
        with self._open_download_response(
            url,
            timeout=timeout,
            no_auth=no_auth,
            additional_headers=additional_headers,
            op_label=op_label,
        ) as response:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f".{op_label}")
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
                                f"{op_label} response exceeded {max_bytes} bytes"
                            )
                finally:
                    tmp.close()
                yield tmp.name
            finally:
                # Best-effort cleanup; never let an unlink failure mask the
                # original exception (or stomp on a successful return).
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass

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
