"""Connect-time SSRF guard for httpx clients.

httpx does not use urllib3, so ``url_safety``'s ``create_connection`` hook never
sees its connections. This module enforces the same policy one layer down, in
httpcore's network backend: ``SafeNetworkBackend.connect_tcp`` resolves the
host, validates each address with the shared ``url_safety`` checks and connects
to the validated IP literal. A pre-call check alone leaves a DNS-rebinding gap
(one lookup is checked, httpx connects with another); checking the address
actually connected to closes it.

TLS is unaffected: httpcore calls ``connect_tcp(host=<origin host>)`` and then
``start_tls(server_hostname=<origin host>)`` on the returned stream, so SNI and
certificate verification still use the original hostname.

With a proxy configured, httpcore connects to the proxy, so the check applies to
the proxy address — the same as the urllib3 hook.

``harden_async_client`` installs the backend on a client httpx has already
built (default transport and env-proxy mounts), so httpx keeps its own proxy,
TLS and SSL-context handling. It relies on httpx/httpcore private attributes
(``AsyncClient._transport`` / ``_mounts``, ``AsyncHTTPTransport._pool``,
``AsyncConnectionPool._network_backend``) and fails closed if they change.
"""

import socket
from typing import Iterable, List, Optional, cast

import anyio
import httpcore
import httpx

from apollo.integrations.http.url_safety import (
    HttpClientError,
    is_blocked_ip,
    iter_connectable_ips,
    preflight_host,
)


class SafeNetworkBackend(httpcore.AsyncNetworkBackend):
    """httpcore network backend that only connects to addresses allowed by the
    ``url_safety`` policy (default tier unless ``strict_ip_policy``).

    The connect loop mirrors ``url_safety._safe_create_connection`` through the
    shared ``preflight_host`` / ``iter_connectable_ips``: addresses are tried
    in ``getaddrinfo`` order, each validated just before it is tried; a blocked
    address raises ``HttpClientError``; a connect failure falls through to the
    next address. The inner backend only ever receives IP literals, never a
    hostname it would re-resolve.

    Every ``HttpClientError`` raised is also appended to ``blocked``, so a
    caller can tell a block happened even when the code driving httpx swallows
    the exception.
    """

    def __init__(
        self,
        inner: Optional[httpcore.AsyncNetworkBackend] = None,
        *,
        strict_ip_policy: bool = False,
    ):
        # cast: pyright sees httpcore's fallback stub (defined when anyio is missing)
        self._inner = inner or cast(
            httpcore.AsyncNetworkBackend, httpcore.AnyIOBackend()
        )
        self._strict_ip_policy = strict_ip_policy
        self.blocked: List[HttpClientError] = []

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: Optional[float] = None,
        local_address: Optional[str] = None,
        socket_options: Optional[Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> httpcore.AsyncNetworkStream:
        try:
            return await self._connect_tcp(
                host, port, timeout, local_address, socket_options
            )
        except HttpClientError as exc:
            self.blocked.append(exc)
            raise

    async def connect_unix_socket(
        self,
        path: str,
        timeout: Optional[float] = None,
        socket_options: Optional[Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> httpcore.AsyncNetworkStream:
        exc = HttpClientError("request refuses unix socket connections")
        self.blocked.append(exc)
        raise exc

    async def sleep(self, seconds: float) -> None:
        await self._inner.sleep(seconds)

    async def _connect_tcp(
        self,
        host: str,
        port: int,
        timeout: Optional[float],
        local_address: Optional[str],
        socket_options: Optional[Iterable[httpcore.SOCKET_OPTION]],
    ) -> httpcore.AsyncNetworkStream:
        # may be tried once per resolved address
        options = list(socket_options) if socket_options is not None else None

        async def connect(ip: str) -> httpcore.AsyncNetworkStream:
            return await self._inner.connect_tcp(
                ip,
                port,
                timeout=timeout,
                local_address=local_address,
                socket_options=options,
            )

        literal = preflight_host(host)
        if literal is not None:
            if is_blocked_ip(literal, strict_ip_policy=self._strict_ip_policy):
                raise HttpClientError(f"request refuses blocked address: {literal}")
            # zone IDs are dropped; they only matter for link-local, which is blocked
            return await connect(str(literal))

        try:
            with anyio.fail_after(timeout):
                infos = await anyio.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except TimeoutError as exc:
            raise httpcore.ConnectTimeout(f"DNS resolution timed out: {host}") from exc
        except socket.gaierror as exc:
            # a lookup that fails here passed the caller's pre-check moments
            # ago, so report it as a connection failure, not a block
            raise httpcore.ConnectError(f"DNS resolution failed: {exc}") from exc

        last_exc: Optional[Exception] = None
        for ip in iter_connectable_ips(infos, strict_ip_policy=self._strict_ip_policy):
            try:
                return await connect(ip)
            except (httpcore.ConnectError, httpcore.ConnectTimeout) as exc:
                last_exc = exc
        if last_exc is not None:
            raise last_exc
        raise httpcore.ConnectError("getaddrinfo returned no usable address")


def harden_async_client(
    client: httpx.AsyncClient, backend: httpcore.AsyncNetworkBackend
) -> None:
    """Install ``backend`` on every transport ``client`` can route through:
    the default transport and each env-proxy mount (``None`` mounts fall back
    to the default transport). Call before the client's first request.

    Fails closed: any transport that isn't a stock ``httpx.AsyncHTTPTransport``
    over an httpcore connection pool raises instead of being left unguarded.

    Raises:
        HttpClientError: a transport the guard can't be installed on.
    """
    transports = [client._transport]
    transports.extend(t for t in client._mounts.values() if t is not None)
    for transport in transports:
        pool = getattr(transport, "_pool", None)
        if (
            not isinstance(transport, httpx.AsyncHTTPTransport)
            or not isinstance(pool, httpcore.AsyncConnectionPool)
            or not hasattr(pool, "_network_backend")
        ):
            raise HttpClientError(
                "request refuses unsupported httpx transport/proxy configuration: "
                f"{type(transport).__name__}"
            )
        pool._network_backend = backend
