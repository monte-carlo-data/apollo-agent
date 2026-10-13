import ipaddress
import os
import socket
import urllib.request
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional
from unittest import TestCase
from unittest.mock import patch

import anyio
import httpcore
import httpx

from apollo.integrations.http import url_safety
from apollo.integrations.http.httpx_safety import (
    SafeNetworkBackend,
    harden_async_client,
)
from apollo.integrations.http.url_safety import HttpClientError

_PUBLIC = "93.184.216.34"
_PUBLIC_2 = "93.184.216.35"
_IMDS = "169.254.169.254"


def _addrinfo(ip: str, port: int = 443) -> tuple:
    family = socket.AF_INET6 if ":" in ip else socket.AF_INET
    return (family, socket.SOCK_STREAM, 6, "", (ip, port))


class _StopAfterTls(Exception):
    pass


class _FakeStream(httpcore.AsyncNetworkStream):
    """Records the TLS server_hostname, then stops the request."""

    def __init__(self) -> None:
        self.server_hostname: Optional[str] = None

    async def start_tls(
        self,
        ssl_context: Any,
        server_hostname: Optional[str] = None,
        timeout: Any = None,
    ) -> httpcore.AsyncNetworkStream:
        self.server_hostname = server_hostname
        raise _StopAfterTls()

    async def aclose(self) -> None:
        pass


class _RecordingBackend(httpcore.AsyncNetworkBackend):
    """Inner backend: records every host it is asked to connect to and plays
    back `results` (a stream, or an exception to raise); unreachable when
    exhausted."""

    def __init__(self, results: Optional[List[Any]] = None) -> None:
        self.calls: List[str] = []
        self._results = list(results or [])

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: Optional[float] = None,
        local_address: Optional[str] = None,
        socket_options: Any = None,
    ) -> httpcore.AsyncNetworkStream:
        self.calls.append(host)
        result = (
            self._results.pop(0)
            if self._results
            else httpcore.ConnectError("unreachable")
        )
        if isinstance(result, BaseException):
            raise result
        return result

    async def sleep(self, seconds: float) -> None:
        pass


@contextmanager
def _resolves(*answers: List[str]) -> Iterator[Any]:
    """Successive getaddrinfo calls return successive answers."""
    with patch(
        "socket.getaddrinfo",
        side_effect=[[_addrinfo(ip) for ip in ips] for ips in answers],
    ) as mock_gai:
        yield mock_gai


@contextmanager
def _proxy_env(env: Dict[str, str]) -> Iterator[None]:
    # clear=True plus env-only getproxies, so the machine's (e.g. macOS system)
    # proxy settings can't leak into the test
    with patch.dict(os.environ, env, clear=True), patch(
        "httpx._utils.getproxies", urllib.request.getproxies_environment
    ):
        yield


def _connect(backend: SafeNetworkBackend, host: str, port: int = 443) -> Any:
    async def run() -> Any:
        return await backend.connect_tcp(host, port, timeout=1)

    return anyio.run(run)


class TestSafeNetworkBackend(TestCase):
    def test_dns_rebinding_is_blocked_at_connect(self):
        # The first lookup (e.g. the pre-call check) sees a public address; the
        # lookup made at connect time sees the metadata service.
        stream = _FakeStream()
        inner = _RecordingBackend([stream])
        backend = SafeNetworkBackend(inner)
        with _resolves([_PUBLIC], [_IMDS]):
            self.assertIs(stream, _connect(backend, "rebind.example.com"))
            with self.assertRaises(HttpClientError):
                _connect(backend, "rebind.example.com")
        self.assertEqual([_PUBLIC], inner.calls)
        self.assertEqual(1, len(backend.blocked))

    def test_blocked_first_address_never_falls_through(self):
        inner = _RecordingBackend([_FakeStream()])
        with _resolves([_IMDS, _PUBLIC]):
            with self.assertRaises(HttpClientError):
                _connect(SafeNetworkBackend(inner), "mixed.example.com")
        self.assertEqual([], inner.calls)

    def test_blocked_after_unreachable_never_connected(self):
        inner = _RecordingBackend([httpcore.ConnectError("down")])
        with _resolves([_PUBLIC, _IMDS]):
            with self.assertRaises(HttpClientError):
                _connect(SafeNetworkBackend(inner), "mixed.example.com")
        self.assertEqual([_PUBLIC], inner.calls)

    def test_falls_back_past_unreachable_address(self):
        stream = _FakeStream()
        inner = _RecordingBackend([httpcore.ConnectTimeout("slow"), stream])
        with _resolves([_PUBLIC, _PUBLIC_2]):
            self.assertIs(stream, _connect(SafeNetworkBackend(inner), "example.com"))
        self.assertEqual([_PUBLIC, _PUBLIC_2], inner.calls)

    def test_all_unreachable_raises_last_error(self):
        last = httpcore.ConnectError("second")
        inner = _RecordingBackend([httpcore.ConnectError("first"), last])
        with _resolves([_PUBLIC, _PUBLIC_2]):
            with self.assertRaises(httpcore.ConnectError) as ctx:
                _connect(SafeNetworkBackend(inner), "example.com")
        self.assertIs(last, ctx.exception)

    def test_no_usable_address_fails_closed(self):
        inner = _RecordingBackend()
        for answer in ([], ["not-an-ip"]):
            with self.subTest(answer=answer):
                with _resolves(answer):
                    with self.assertRaises(httpcore.ConnectError):
                        _connect(SafeNetworkBackend(inner), "example.com")
        self.assertEqual([], inner.calls)

    def test_dns_failure_is_a_connect_error(self):
        backend = SafeNetworkBackend(_RecordingBackend())
        with patch("socket.getaddrinfo", side_effect=socket.gaierror("nope")):
            with self.assertRaises(httpcore.ConnectError):
                _connect(backend, "missing.example.com")
        self.assertEqual([], backend.blocked)

    def test_blocked_literals(self):
        literals = (
            _IMDS,
            "127.0.0.1",
            "::1",
            "[::1]",
            "fe80::1%eth0",
            "::ffff:169.254.169.254",  # IPv4-mapped
            "2002:a9fe:a9fe::1",  # 6to4
            "64:ff9b::169.254.169.254",  # NAT64
            "localhost",
            "localhost.",
        )
        for host in literals:
            with self.subTest(host=host):
                inner = _RecordingBackend()
                with patch("socket.getaddrinfo") as mock_gai:
                    with self.assertRaises(HttpClientError):
                        _connect(SafeNetworkBackend(inner), host)
                mock_gai.assert_not_called()
                self.assertEqual([], inner.calls)

    def test_hostname_resolving_to_embedded_ipv4(self):
        for ip in ("::ffff:169.254.169.254", "2002:a9fe:a9fe::1", "64:ff9b::7f00:1"):
            with self.subTest(ip=ip):
                inner = _RecordingBackend()
                with _resolves([ip]):
                    with self.assertRaises(HttpClientError):
                        _connect(SafeNetworkBackend(inner), "attacker.example.com")
                self.assertEqual([], inner.calls)

    def test_allowed_literal_connects_to_literal(self):
        stream = _FakeStream()
        inner = _RecordingBackend([stream])
        with patch("socket.getaddrinfo") as mock_gai:
            self.assertIs(stream, _connect(SafeNetworkBackend(inner), "10.0.0.5"))
        mock_gai.assert_not_called()
        self.assertEqual(["10.0.0.5"], inner.calls)

    def test_extra_blocked_cidrs_apply(self):
        inner = _RecordingBackend()
        with patch.object(
            url_safety, "_EXTRA_NETWORKS", [ipaddress.ip_network("10.50.0.0/16")]
        ):
            with _resolves(["10.50.1.1"]):
                with self.assertRaises(HttpClientError):
                    _connect(SafeNetworkBackend(inner), "internal.example.com")
        self.assertEqual([], inner.calls)

    def test_strict_tier(self):
        inner = _RecordingBackend()
        with self.assertRaises(HttpClientError):
            _connect(SafeNetworkBackend(inner, strict_ip_policy=True), "10.0.0.5")
        self.assertEqual([], inner.calls)

    def test_unix_socket_refused(self):
        backend = SafeNetworkBackend(_RecordingBackend())

        async def run() -> Any:
            return await backend.connect_unix_socket("/var/run/docker.sock")

        with self.assertRaises(HttpClientError):
            anyio.run(run)
        self.assertEqual(1, len(backend.blocked))

    def test_block_is_not_an_oserror(self):
        # The connect loop falls through only on connect errors; a block that
        # became an OSError / httpcore error would be skipped past.
        self.assertFalse(issubclass(HttpClientError, OSError))
        self.assertFalse(issubclass(HttpClientError, httpcore.ConnectError))


class TestHardenAsyncClient(TestCase):
    def _send(self, client: httpx.AsyncClient, url: str) -> None:
        async def run() -> None:
            async with client:
                await client.get(url)

        anyio.run(run)

    def test_tls_uses_original_hostname(self):
        stream = _FakeStream()
        inner = _RecordingBackend([stream])
        with _proxy_env({}):
            client = httpx.AsyncClient()
        harden_async_client(client, SafeNetworkBackend(inner))
        with _resolves([_PUBLIC]):
            with self.assertRaises(_StopAfterTls):
                self._send(client, "https://mcp.example.com/mcp")
        self.assertEqual([_PUBLIC], inner.calls)
        self.assertEqual("mcp.example.com", stream.server_hostname)

    def test_block_passes_through_client_unchanged(self):
        inner = _RecordingBackend()
        with _proxy_env({}):
            client = httpx.AsyncClient()
        harden_async_client(client, SafeNetworkBackend(inner))
        with _resolves([_IMDS]):
            with self.assertRaises(HttpClientError):
                self._send(client, "https://rebind.example.com/mcp")
        self.assertEqual([], inner.calls)

    def test_proxy_address_is_checked(self):
        # Proxied requests connect to the proxy, so the proxy is what's checked
        # (parity with the urllib3 hook).
        inner = _RecordingBackend()
        with _proxy_env({"HTTPS_PROXY": "http://proxy.example.com:3128"}):
            client = httpx.AsyncClient()
        harden_async_client(client, SafeNetworkBackend(inner))
        with _resolves([_IMDS]) as mock_gai:
            with self.assertRaises(HttpClientError):
                self._send(client, "https://mcp.example.com/mcp")
        # anyio IDNA-encodes the host before resolving
        self.assertEqual(b"proxy.example.com", mock_gai.call_args.args[0])
        self.assertEqual([], inner.calls)

    def test_every_transport_gets_the_backend(self):
        env = {
            "HTTP_PROXY": "http://proxy.example.com:3128",
            "HTTPS_PROXY": "http://proxy.example.com:3129",
            "ALL_PROXY": "http://proxy.example.com:3130",
            "NO_PROXY": "internal.example.com",
        }
        backend = SafeNetworkBackend(_RecordingBackend())
        with _proxy_env(env):
            client = httpx.AsyncClient()
        harden_async_client(client, backend)
        transports = [client._transport]
        transports.extend(t for t in client._mounts.values() if t is not None)
        self.assertGreater(len(transports), 1)
        for transport in transports:
            self.assertIs(backend, transport._pool._network_backend)  # type: ignore

    def test_unsupported_transport_fails_closed(self):
        client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: None))  # type: ignore
        with self.assertRaises(HttpClientError):
            harden_async_client(client, SafeNetworkBackend(_RecordingBackend()))
