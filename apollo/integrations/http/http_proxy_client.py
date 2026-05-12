import base64
import io
import ipaddress
import json
import logging
import os
import tempfile
import zipfile
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import urlsplit

import requests
from requests import HTTPError
from retry.api import retry_call

from apollo.common.agent.models import AgentOperation
from apollo.common.agent.redact import AgentRedactUtilities
from apollo.integrations.base_proxy_client import BaseProxyClient
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

# YET-1229: structural locations inside a Mule application JAR that
# ``extract_mulesoft_sources`` reads. The manifest names the XML configs to
# extract; everything under ``properties/`` is included verbatim so the
# downstream parser can resolve ``${...}`` placeholders.
_MULESOFT_ARTIFACT_MANIFEST = "META-INF/mule-artifact/mule-artifact.json"
_MULESOFT_PROPERTIES_PREFIX = "properties/"


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

    @contextmanager
    def _open_download_response(
        self,
        url: str,
        timeout: int,
        no_auth: bool,
        additional_headers: Optional[Dict],
        op_label: str,
    ) -> Iterator[requests.Response]:
        """Open a streaming GET response with the full SSRF + redirect + status guards.

        Use as a context manager: ``with self._open_download_response(...) as response:``.
        On every error-exit path the response is closed internally before the
        exception propagates; on success, the context manager closes it on exit.
        ``op_label`` is included in error messages so the user-facing message
        names the calling method.

        Raises:
            HttpClientError: SSRF guard rejection, transport error, 3xx redirect,
                or 4xx response.
            HTTPError: 5xx response.
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
        with self._open_download_response(
            url,
            timeout=timeout,
            no_auth=no_auth,
            additional_headers=additional_headers,
            op_label="download_to_storage",
        ) as response:
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
                                f"download_to_storage response exceeded {max_bytes} bytes"
                            )
                finally:
                    tmp.close()
                # Cached on self after first call — avoids reconstructing the
                # SDK client (boto3 / GCS / Azure) per request.
                self._get_storage_client().upload_file(storage_key, tmp.name)
            finally:
                # Best-effort cleanup; never let an unlink failure mask the
                # original exception (or stomp on a successful return).
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass

        return storage_key

    def extract_mulesoft_sources(
        self,
        url: str,
        timeout: int = 300,
        max_bytes: Optional[int] = None,
        no_auth: bool = True,
        additional_headers: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Stream a Mule application JAR from ``url`` to a transient tempfile,
        extract just the flow source files (the XML configs listed in
        ``META-INF/mule-artifact/mule-artifact.json::configs[]`` plus every
        entry under ``properties/``), repackage them into a small in-memory
        deflate zip, and return the result inline. **No agent-storage write.**
        The JAR exists only as a transient tempfile and is deleted on every
        exit path.

        This is the server-side companion to data-collector's YET-1215
        architecture: DC parses MuleSoft flow XML in-process, so all the
        agent needs to ship back is the small sources zip (~25 KB typical
        for a 100 MB JAR). DC then unzips locally, parses, and ships a
        ``parsed_app`` dict on its Kinesis event — the 100 MB JAR never
        crosses the JSON-base64 agent transport.

        Same streaming-download safety surface as ``download_to_storage``:
        HTTPS-only, non-public IP literals rejected, redirects refused,
        ``max_bytes`` cap fires mid-stream, error messages strip the URL,
        connection released on every exit path.

        Return shape (the DC contract — see YET-1229):

        * ``sources_zip_b64``: ``str | None`` — base64-encoded deflate zip
          of the extracted XML configs + ``properties/*`` files. ``None`` on
          extraction failure.
        * ``sources_size_bytes``: ``int | None`` — pre-base64 zip size, for
          ops visibility. ``None`` on extraction failure.
        * ``sources_extraction_status``: ``"ok"`` (sources extracted and
          inlined) or ``"extraction_failed"`` (missing / corrupt manifest,
          unreadable JAR). A specific ``configs[]`` entry that's missing
          from the JAR is *not* a failure — it's logged and skipped, and
          the op still returns ``"ok"`` with the entries it could read.

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
            op_label="extract_mulesoft_sources",
        ) as response:
            tmp = tempfile.NamedTemporaryFile(
                delete=False, suffix=".extract_mulesoft_sources"
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
                                f"extract_mulesoft_sources response exceeded {max_bytes} bytes"
                            )
                finally:
                    tmp.close()
                zip_bytes, status = _extract_mulesoft_sources_from_jar(tmp.name)
            finally:
                # Best-effort cleanup; never let an unlink failure mask the
                # original exception (or stomp on a successful return).
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass

        return {
            "sources_zip_b64": (
                base64.b64encode(zip_bytes).decode("ascii") if zip_bytes is not None else None
            ),
            "sources_size_bytes": len(zip_bytes) if zip_bytes is not None else None,
            "sources_extraction_status": status,
        }

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


def _extract_mulesoft_sources_from_jar(tmp_path: str) -> Tuple[Optional[bytes], str]:
    """Open a downloaded Mule application JAR + repackage its flow sources.

    Returns ``(zip_bytes, status)`` where ``status`` is one of:

    * ``"ok"`` — extraction succeeded; ``zip_bytes`` is the in-memory
      deflate zip containing the listed config XMLs + every entry under
      ``properties/``.
    * ``"extraction_failed"`` — JAR was unreadable, the manifest was missing
      or corrupt, or some other unrecoverable error. ``zip_bytes`` is
      ``None`` in this case; the caller emits ``sources_zip_b64=None``.

    Individual ``configs[]`` entries that are listed in the manifest but
    missing from the JAR are logged as warnings and skipped — the op still
    returns ``"ok"`` with whatever entries it could read. The downstream
    parser is tolerant of partial config sets; failing the whole op for
    one stale manifest reference would lose more lineage than it preserves.
    """
    try:
        with zipfile.ZipFile(tmp_path) as jar:
            try:
                manifest_bytes = jar.read(_MULESOFT_ARTIFACT_MANIFEST)
            except KeyError:
                _logger.warning(
                    "mulesoft_sources_missing_manifest: %s not found in JAR",
                    _MULESOFT_ARTIFACT_MANIFEST,
                )
                return None, "extraction_failed"
            try:
                manifest = json.loads(manifest_bytes)
            except json.JSONDecodeError:
                _logger.exception(
                    "mulesoft_sources_corrupt_manifest: %s was not valid JSON",
                    _MULESOFT_ARTIFACT_MANIFEST,
                )
                return None, "extraction_failed"

            extracted: List[Tuple[str, bytes]] = []

            for name in manifest.get("configs") or []:
                try:
                    extracted.append((name, jar.read(name)))
                except KeyError:
                    _logger.warning(
                        "mulesoft_sources_missing_config_entry: configs[] referenced %s "
                        "but the entry is not in the JAR; skipping",
                        name,
                    )

            for info in jar.infolist():
                if info.is_dir():
                    continue
                if info.filename.startswith(_MULESOFT_PROPERTIES_PREFIX):
                    extracted.append((info.filename, jar.read(info.filename)))
    except zipfile.BadZipFile:
        _logger.exception(
            "mulesoft_sources_bad_zip: extract_mulesoft_sources received a non-ZIP body",
        )
        return None, "extraction_failed"

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as repack:
        for name, content in extracted:
            repack.writestr(name, content)
    return out.getvalue(), "ok"
