"""SSRF defense for the agent's HTTP integrations.

The guard is enforced inside urllib3's TCP-connect path, gated by a
per-thread policy that the caller activates around the request via
``safety_policy``. Because urllib3 calls ``create_connection`` for every
connection it opens — initial request, redirected request, retry — a
single wrapper on that function covers every hop without any special
``requests.Session`` plumbing.

Two policy tiers share one implementation:

  * **Default policy** (used by ``do_request`` and similar integrations
    where the agent talks to customer-configured services): rejects
    cloud metadata services and loopback, but allows RFC1918 so the
    agent can reach databases and services in the customer's VPC/VNet
    via private IP. Operator-extensible via ``MCD_HTTP_BLOCKED_CIDRS``.

  * **Strict policy** (used by streaming download endpoints whose URLs
    originate further upstream — e.g. pre-signed S3/GCS/Azure Blob URLs
    handed to ``HttpProxyClient.download_*``): rejects every non-public
    address (RFC1918, link-local, loopback, multicast, reserved,
    unspecified) and refuses non-HTTPS. Enabled by passing
    ``strict_ip_policy=True`` and ``https_only=True`` to
    ``safety_policy``.

Default-blocked CIDRs cover cloud metadata services and other
addresses the agent never has a legitimate reason to reach:

  - 169.254.0.0/16  IPv4 link-local (AWS/GCP/Azure/Oracle/etc. metadata)
  - fe80::/10       IPv6 link-local
  - 127.0.0.0/8     IPv4 loopback
  - ::1/128         IPv6 loopback

Operators can extend the default block list via the
``MCD_HTTP_BLOCKED_CIDRS`` env var: a comma-separated list of CIDRs
(e.g. ``"100.64.0.0/10,10.50.0.0/16"``). Invalid entries are logged
and skipped at import time so a typo never crashes the agent. The
env-var extension applies on top of both policy tiers. Read once at
module-import time — changes require a process restart.

Operators can also enforce HTTPS in the default policy tier by setting
``MCD_HTTP_REQUIRE_HTTPS=true``. The strict download tier (which passes
``https_only=True`` explicitly) is unaffected — it already requires
HTTPS unconditionally. Only the default tier picks up the env-var policy.
Read once at module-import time — changes require a process restart.

The ``create_connection`` wrapper is **always installed** at import
time but is a passthrough whenever no policy is active on the current
thread, so unrelated urllib3 users in the same process (Snowflake
connector, GCS / Azure SDKs, OpenTelemetry exporter, etc.) see the
original behavior.

Out of scope:
  - DB clients (Snowflake, BigQuery, Redshift, Starburst, etc.) — they
    speak DB-specific wire protocols, so a metadata-service IP fails
    handshake rather than serving credentials. Different threat model.
"""

from __future__ import annotations

import ipaddress
import logging
import os
import socket
import threading
from contextlib import contextmanager
from typing import Any, Iterator, List, Tuple, Union
from urllib.parse import urlsplit

import requests
from urllib3.util import connection as _urllib3_connection

_logger = logging.getLogger(__name__)


class HttpClientError(Exception):
    """Raised when a request URL or its resolved IP fails the SSRF
    safety check.

    Defined here (rather than in ``http_proxy_client``) so that
    ``url_safety`` has no upward dependency on the client module.
    ``http_proxy_client`` re-exports it under the same name to
    preserve the existing import surface for external callers.

    Note: ``HttpClientError`` deliberately does NOT inherit from
    ``OSError``. When raised from inside ``create_connection`` it
    propagates cleanly through urllib3's pool layer and the
    ``HTTPAdapter`` (both of which catch ``OSError`` / urllib3-specific
    exceptions but not arbitrary subclasses), so callers see the
    original ``HttpClientError`` rather than a wrapped
    ``ConnectionError``.
    """


_ENV_EXTRA_BLOCKED_CIDRS = "MCD_HTTP_BLOCKED_CIDRS"
_ENV_REQUIRE_HTTPS = "MCD_HTTP_REQUIRE_HTTPS"

# Sentinel for idempotency guard on the create_connection wrapper. Using a
# private object() instance prevents any external code from accidentally
# setting an attribute of the same name with a truthy value — which would
# cause a silent SSRF regression by skipping the install.
_MC_SSRF_GUARD_SENTINEL = object()

_DEFAULT_BLOCKED_CIDRS: Tuple[str, ...] = (
    "169.254.0.0/16",
    "fe80::/10",
    "127.0.0.0/8",
    "::1/128",
)

_Network = Union[ipaddress.IPv4Network, ipaddress.IPv6Network]
_Address = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]


def _parse_cidrs(cidrs: Tuple[str, ...], *, source: str) -> List[_Network]:
    parsed: List[_Network] = []
    for raw in cidrs:
        entry = raw.strip()
        if not entry:
            continue
        try:
            parsed.append(ipaddress.ip_network(entry, strict=False))
        except ValueError as exc:
            _logger.warning(
                "url_safety: ignoring invalid CIDR in %s: '%s' (%s)",
                source,
                entry,
                exc,
            )
    return parsed


def _load_extra_blocked_networks() -> List[_Network]:
    raw = os.environ.get(_ENV_EXTRA_BLOCKED_CIDRS, "")
    if not raw:
        return []
    return _parse_cidrs(tuple(raw.split(",")), source=_ENV_EXTRA_BLOCKED_CIDRS)


def _load_bool_env(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    if raw in ("true", "1", "yes", "on"):
        return True
    if raw in ("false", "0", "no", "off"):
        return False
    _logger.warning(
        "url_safety: ignoring invalid value for %s: '%s' — using default %s",
        name,
        raw,
        default,
    )
    return default


_DEFAULT_NETWORKS: List[_Network] = _parse_cidrs(
    _DEFAULT_BLOCKED_CIDRS, source="defaults"
)
_EXTRA_NETWORKS: List[_Network] = _load_extra_blocked_networks()
_REQUIRE_HTTPS_BY_DEFAULT: bool = _load_bool_env(_ENV_REQUIRE_HTTPS, default=False)


def _log_startup_summary() -> None:
    _logger.info(
        "url_safety: SSRF guard initialized — %d default CIDR(s), %d extra CIDR(s) from %s, https_only=%s",
        len(_DEFAULT_NETWORKS),
        len(_EXTRA_NETWORKS),
        _ENV_EXTRA_BLOCKED_CIDRS,
        _REQUIRE_HTTPS_BY_DEFAULT,
    )


_log_startup_summary()


def _ip_is_rejected(ip: _Address, *, strict_ip_policy: bool) -> bool:
    """Return True if ``ip`` is disallowed by the active policy tier.

    The env-var extra list applies under both tiers; the strict tier
    adds the broader "non-public" rejection on top.
    """
    if any(ip in net for net in _DEFAULT_NETWORKS):
        return True
    if any(ip in net for net in _EXTRA_NETWORKS):
        return True
    if strict_ip_policy:
        # ``is_global`` is False for private (RFC1918), loopback,
        # link-local, reserved, and unspecified — but Python's
        # ipaddress module reports ``is_global=True`` for multicast,
        # so multicast gets an explicit reject.
        if (not ip.is_global) or ip.is_multicast:
            return True
    return False


def assert_safe_destination(
    host: str,
    port: int,
    *,
    strict_ip_policy: bool = False,
) -> None:
    """Validate that ``(host, port)`` is a safe destination under the
    active block list, without opening any connection.

    Used by callers that issue raw socket / telnet probes (i.e. anything
    not going through ``requests`` / ``urllib3``). The URL-layer
    pre-flight does not apply (no scheme) — this helper only checks
    host validity + IP block list. Hostnames are resolved via
    ``getaddrinfo`` and the whole hostname is rejected if any A/AAAA
    record is blocked (no fallback connect to defer to here, unlike
    the ``create_connection`` wrapper).

    Args:
        host: destination hostname or IP literal. Empty / ``"localhost"``
            are rejected with a clearer message than the IP-block would
            produce.
        port: destination port (passed to ``getaddrinfo`` for hostname
            resolution; not otherwise inspected).
        strict_ip_policy: when True, reject every non-public IP (RFC1918,
            link-local, loopback, multicast, reserved, unspecified).
            Default False — only the explicit block list applies.

    Raises:
        HttpClientError: empty/localhost host, blocked IP literal, or
            blocked IP in the resolved record set.
    """
    if not host:
        raise HttpClientError("destination refuses '<empty>' host")
    host_lower = host.lower()
    if host_lower == "localhost":
        raise HttpClientError("destination refuses 'localhost' host")

    try:
        literal = ipaddress.ip_address(host_lower.strip("[]"))
    except ValueError:
        literal = None
    if literal is not None:
        if _ip_is_rejected(literal, strict_ip_policy=strict_ip_policy):
            raise HttpClientError(f"destination refuses blocked address: {literal}")
        return

    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise HttpClientError(f"DNS resolution failed: {exc}") from None

    for info in infos:
        ip_str = str(info[4][0])
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        if _ip_is_rejected(ip, strict_ip_policy=strict_ip_policy):
            raise HttpClientError(
                f"destination refuses blocked address resolved from hostname: {ip}"
            )


def _assert_safe_url_scheme_and_host(url: str, *, https_only: bool) -> None:
    """URL-layer pre-flight: scheme + host sanity.

    ``create_connection`` only sees ``(host, port)`` — port 443 is not a
    reliable proxy for "this is HTTPS", so the scheme check has to live
    at the URL layer. The empty/``localhost`` check is technically
    redundant (``localhost`` resolves to 127.0.0.1 → caught by the IP
    block) but produces a clearer error message at the URL layer.
    """
    parts = urlsplit(url)
    effective_https_only = https_only or _REQUIRE_HTTPS_BY_DEFAULT
    allowed_schemes = ("https",) if effective_https_only else ("http", "https")
    if parts.scheme not in allowed_schemes:
        raise HttpClientError(f"request refuses unsupported scheme '{parts.scheme}'")
    host = (parts.hostname or "").lower()
    if host in ("", "localhost"):
        raise HttpClientError(f"request refuses '{host or '<empty>'}' host")


# --- Per-thread policy + create_connection wrapper ---------------------------
#
# The wrapper around ``urllib3.util.connection.create_connection`` is
# installed once at import time. It is a passthrough whenever no policy
# is active on the current thread, so unrelated urllib3 users in the
# same process (Snowflake connector, GCS / Azure SDKs, OpenTelemetry
# exporter, etc.) see the original behavior.
#
# When a policy IS active (set via ``safety_policy``), the wrapper:
#   1. Validates IP literals directly against the active tier.
#   2. For hostnames, resolves and validates every A/AAAA record —
#      rejecting the whole hostname if any record is blocked, then
#      iterating through the validated list trying to connect. This
#      preserves urllib3's native multi-IP fallback semantics.
#
# Because the wrapper runs on every ``create_connection``, redirected
# requests get the same guard as the initial request without any
# ``requests.Session`` subclassing.

_policy = threading.local()
# We hook urllib3.util.connection.create_connection because it is the
# function every HTTP(S) connection ultimately calls for the TCP socket
# create+connect step. This is internal urllib3 API; the hook has been
# verified against urllib3 1.26.x and 2.x. If a future urllib3 version
# reorganizes connection creation away from this function, the hook
# will silently become a no-op — `test_hook_is_installed` is the
# canary for that.
_original_create_connection = _urllib3_connection.create_connection


def _safe_create_connection(address: Tuple[str, int], *args: Any, **kwargs: Any) -> Any:
    if not getattr(_policy, "active", False):
        return _original_create_connection(address, *args, **kwargs)

    host, port = address
    strict = getattr(_policy, "strict_ip_policy", False)

    # IP literal: validate, then delegate to the original (which is now
    # a cheap getaddrinfo on a literal + connect).
    try:
        literal = ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        literal = None
    if literal is not None:
        if _ip_is_rejected(literal, strict_ip_policy=strict):
            raise HttpClientError(f"request refuses blocked address: {literal}")
        # Pass the original `address` tuple through unchanged: when the IP is
        # not rejected, urllib3 sees exactly what it would have without the
        # wrapper. Reconstructing here (e.g. `(str(literal), port)`) would
        # work — but minimizing interference avoids subtle interpretation
        # drift between the validated and pass-through paths.
        return _original_create_connection(address, *args, **kwargs)

    # Hostname: resolve, then validate-and-connect in a single pass.
    # If a transiently-unreachable address fails, fall back to the next
    # (parity with the original urllib3 behavior). We only validate an
    # address when we're about to connect to it — addresses past a
    # successful connect are not validated, since we never reach them.
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise HttpClientError(f"DNS resolution failed: {exc}") from None

    err: Union[BaseException, None] = None
    for info in infos:
        # info[4] is the sockaddr tuple; first element is the IP literal as
        # a string (str() is a no-op typed-narrowing here, since getaddrinfo
        # always returns IP literals as strings for AF_INET / AF_INET6).
        ip_str = str(info[4][0])
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        if _ip_is_rejected(ip, strict_ip_policy=strict):
            raise HttpClientError(
                f"request refuses blocked address resolved from hostname: {ip}"
            )
        try:
            return _original_create_connection((ip_str, port), *args, **kwargs)
        except OSError as exc:
            err = exc
    if err is not None:
        raise err
    raise OSError("getaddrinfo returned no usable address")


_safe_create_connection._mc_ssrf_guard = (
    _MC_SSRF_GUARD_SENTINEL  # marker for idempotency check
)

if (
    getattr(_urllib3_connection.create_connection, "_mc_ssrf_guard", None)
    is not _MC_SSRF_GUARD_SENTINEL
):
    _urllib3_connection.create_connection = _safe_create_connection
# else: module already imported (possibly via a different import path);
# don't re-wrap. _original_create_connection retains the value from
# the first import.
#
# Note: under ``importlib.reload(...)``, the captured
# ``_original_create_connection`` would point at the already-installed
# wrapper. The marker check correctly short-circuits installation in that
# case, but ``reload`` is not a supported recovery path — restart the
# process.


@contextmanager
def safety_policy(
    url: Union[str, None] = None,
    *,
    strict_ip_policy: bool = False,
    https_only: bool = False,
) -> Iterator[None]:
    """Activate the SSRF policy for HTTP requests on this thread for the
    duration of the context.

    If ``url`` is provided, the URL is validated upfront (scheme + host)
    against the active tier — this is the right entry point for code
    that's about to issue a request and wants both the URL pre-flight
    and the IP-level guard in one call.

    The IP-level check runs automatically via the
    ``create_connection`` wrapper for every TCP connect made inside
    the context — initial request, redirected request, retry — so
    callers do not need to subclass ``requests.Session`` to handle
    cross-host redirects safely.

    ``https_only`` is enforced ONLY against the entry URL via the
    URL-layer pre-flight; it is NOT re-checked on redirected hops (the
    TCP-layer hook does not see the URL scheme). Callers that need to
    prevent redirect-based scheme downgrade MUST also pass
    ``allow_redirects=False`` to the underlying ``requests`` call. The
    strict download path in ``HttpProxyClient._open_download_response``
    already does this.

    The policy is per-thread (``threading.local``). It applies only to
    HTTP calls issued on the same thread that entered ``safety_policy``.
    If the calling code dispatches the HTTP request to a worker thread
    (``concurrent.futures.ThreadPoolExecutor``) or via
    ``asyncio.get_event_loop().run_in_executor(...)``, the worker thread
    will see no active policy and the guard will be a passthrough.
    Callers that fan out HTTP work to other threads MUST re-enter
    ``safety_policy`` on the worker thread.

    Args:
        url: optional URL whose scheme + host should be validated
            upfront against the active tier.
        strict_ip_policy: when True, reject every non-public IP under
            this context (used by the download tier).
        https_only: when True, reject anything other than HTTPS under
            this context (used by the download tier).

    Raises:
        HttpClientError: ``url`` (when given) fails the scheme/host
            check, or any TCP connect made inside the context targets
            a blocked address.
    """
    prev_active = getattr(_policy, "active", False)
    prev_strict = getattr(_policy, "strict_ip_policy", False)
    _policy.active = True
    _policy.strict_ip_policy = strict_ip_policy
    try:
        if url is not None:
            _assert_safe_url_scheme_and_host(url, https_only=https_only)
        yield
    finally:
        _policy.active = prev_active
        _policy.strict_ip_policy = prev_strict


def safe_request(method: str, url: str, **kwargs: Any) -> requests.Response:
    """Validate ``url`` and issue the request with SSRF guards active.

    Drop-in for ``requests.request(method, url, **kwargs)`` — same
    signature and same return type. Uses the default (permissive)
    policy tier — RFC1918 allowed. For the strict download tier,
    wrap a direct ``requests.get`` / ``requests.head`` call in
    ``safety_policy(url, strict_ip_policy=True, https_only=True)``.

    Redirects are followed safely: every hop goes through
    ``create_connection``, every hop is validated against the same
    policy as the initial URL.

    Raises:
        HttpClientError: URL or any resolved/redirect address rejected
            by the active policy.
    """
    with safety_policy(url):
        return requests.request(method, url, **kwargs)
