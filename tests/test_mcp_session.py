import http.server
import json
import logging
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Dict, Iterator, List, Optional
from unittest import TestCase
from unittest.mock import AsyncMock, patch

import anyio
import httpcore
import httpx
from botocore.credentials import Credentials
from mcp import types
from mcp.server.lowlevel import Server
from mcp.shared.exceptions import McpError
from mcp.shared.memory import create_client_server_memory_streams

from apollo.integrations.http import url_safety
from apollo.integrations.http.httpx_safety import harden_async_client
from apollo.integrations.mcp.auth import ResolvedAuth, SigV4HttpxAuth
from apollo.integrations.mcp.errors import McpClientError, McpErrorCode
from apollo.integrations.mcp.session import (
    DEFAULT_MAX_RESULT_BYTES,
    DEFAULT_TIMEOUT_SECONDS,
    McpLimits,
    run_operation,
)
from tests.ssrf_helpers import addrinfo, isolated_env

_URL = "https://aws-mcp.us-east-1.api.aws/mcp"


# ---- in-memory server -------------------------------------------------------


def _memory_server(server_requests: Optional[List[str]] = None) -> Server:
    server: Server = Server("test")
    tools = [
        types.Tool(
            name=f"tool_{i}", description="d" * 300, inputSchema={"type": "object"}
        )
        for i in range(5)
    ]

    @server.list_tools()
    async def list_tools(request: types.ListToolsRequest) -> types.ListToolsResult:
        # the server also calls this itself (request=None) to look up tool schemas
        params = request.params if request else None
        start = int(params.cursor) if params and params.cursor else 0
        page = tools[start : start + 2]
        next_cursor = str(start + 2) if start + 2 < len(tools) else None
        return types.ListToolsResult(tools=page, nextCursor=next_cursor)

    @server.call_tool()
    async def call_tool(name: str, arguments: Dict[str, Any]):
        if name == "echo":
            return types.CallToolResult(
                content=[types.TextContent(type="text", text=json.dumps(arguments))],
                structuredContent={"echo": arguments},
            )
        if name == "slow":
            await anyio.sleep(10)
        if name == "big":
            return [types.TextContent(type="text", text="x" * 50_000)]
        if name == "ask_client":
            session = server.request_context.session
            for label, request in (
                (
                    "sampling",
                    lambda: session.create_message(
                        messages=[
                            types.SamplingMessage(
                                role="user",
                                content=types.TextContent(type="text", text="hi"),
                            )
                        ],
                        max_tokens=5,
                    ),
                ),
                ("roots", session.list_roots),
                (
                    "elicitation",
                    lambda: session.elicit(
                        message="?",
                        requestedSchema={"type": "object", "properties": {}},
                    ),
                ),
            ):
                try:
                    await request()
                    server_requests.append(f"{label}: answered")  # type: ignore[union-attr]
                except McpError as exc:
                    server_requests.append(f"{label}: {exc.error.message}")  # type: ignore[union-attr]
            return [types.TextContent(type="text", text="done")]
        raise ValueError(f"unknown tool {name}")

    return server


def _memory_streams(server: Server):
    @asynccontextmanager
    async def factory(client, url, session_id, protocol_version, terminate_on_close):
        async with create_client_server_memory_streams() as (client_side, server_side):
            async with anyio.create_task_group() as tg:
                tg.start_soon(
                    lambda: server.run(
                        server_side[0],
                        server_side[1],
                        server.create_initialization_options(),
                    )
                )
                yield client_side[0], client_side[1], lambda: "memory-session"
                tg.cancel_scope.cancel()

    return factory


def _run(server: Server, operation: str, **kwargs) -> Dict[str, Any]:
    return run_operation(
        _URL,
        ResolvedAuth(),
        operation,
        streams_factory=_memory_streams(server),
        **kwargs,
    )


class TestInMemory(TestCase):
    def test_list_tools_follows_pages(self):
        result = _run(_memory_server(), "list_tools")
        self.assertEqual(
            [f"tool_{i}" for i in range(5)], [t["name"] for t in result["tools"]]
        )
        self.assertFalse(result["truncated"])

    def test_list_tools_capped(self):
        result = _run(
            _memory_server(), "list_tools", limits=McpLimits(max_result_bytes=1_000)
        )
        self.assertTrue(result["truncated"])
        self.assertLess(len(result["tools"]), 5)

    def test_call_tool(self):
        result = _run(
            _memory_server(),
            "call_tool",
            tool="echo",
            arguments={"code": "result = 1"},
            keep_session=True,
        )
        self.assertEqual(
            [{"type": "text", "text": '{"code": "result = 1"}'}], result["content"]
        )
        self.assertEqual({"echo": {"code": "result = 1"}}, result["structured_content"])
        self.assertFalse(result["is_error"])
        self.assertFalse(result["truncated"])
        self.assertEqual("memory-session", result["session_id"])
        self.assertFalse(result["session_resumed"])
        self.assertEqual(
            {"assume_role_ms", "initialize_ms", "operation_ms", "total_ms"},
            set(result["timings"]),
        )
        self.assertEqual(result["timings"]["total_ms"], result["duration_ms"])

    def test_tool_errors_are_results(self):
        result = _run(_memory_server(), "call_tool", tool="nope")
        self.assertTrue(result["is_error"])

    def test_call_tool_capped(self):
        result = _run(
            _memory_server(),
            "call_tool",
            tool="big",
            limits=McpLimits(max_result_bytes=2_000),
        )
        self.assertTrue(result["truncated"])
        self.assertLessEqual(len(json.dumps(result)), 2_000)

    def test_server_initiated_requests_are_declined(self):
        server_requests: List[str] = []
        result = _run(_memory_server(server_requests), "call_tool", tool="ask_client")
        self.assertFalse(result["is_error"])
        self.assertEqual(
            [
                "sampling: Sampling not supported",
                "roots: List roots not supported",
                "elicitation: Elicitation not supported",
            ],
            server_requests,
        )

    def test_timeout(self):
        with self.assertRaises(McpClientError) as ctx:
            _run(
                _memory_server(),
                "call_tool",
                tool="slow",
                limits=McpLimits(timeout_seconds=1),
            )
        self.assertEqual(McpErrorCode.AGENT_TIMEOUT, ctx.exception.code)

    def test_bad_requests(self):
        for kwargs in ({"operation": "delete_everything"}, {"operation": "call_tool"}):
            with self.assertRaises(McpClientError) as ctx:
                run_operation(_URL, ResolvedAuth(), **kwargs)
            self.assertEqual(McpErrorCode.BAD_REQUEST, ctx.exception.code)


class TestLimits(TestCase):
    def test_defaults(self):
        limits = McpLimits.from_dict(None)
        self.assertEqual(DEFAULT_TIMEOUT_SECONDS, limits.timeout_seconds)
        self.assertEqual(DEFAULT_MAX_RESULT_BYTES, limits.max_result_bytes)

    def test_clamped(self):
        self.assertEqual(
            McpLimits(timeout_seconds=1, max_result_bytes=1_000),
            McpLimits.from_dict({"timeout_seconds": 0, "max_result_bytes": 5}),
        )
        self.assertEqual(
            McpLimits(timeout_seconds=300, max_result_bytes=1_000_000),
            McpLimits.from_dict({"timeout_seconds": 10_000, "max_result_bytes": 10**9}),
        )
        self.assertEqual(
            McpLimits(timeout_seconds=12.5, max_result_bytes=50_000),
            McpLimits.from_dict(
                {"timeout_seconds": "12.5", "max_result_bytes": 50_000}
            ),
        )

    def test_invalid(self):
        for limits in ({"timeout_seconds": "soon"}, {"max_result_bytes": None}, "x"):
            with self.assertRaises(McpClientError, msg=str(limits)) as ctx:
                McpLimits.from_dict(limits)  # type: ignore[arg-type]
            self.assertEqual(McpErrorCode.BAD_REQUEST, ctx.exception.code)


# ---- fake streamable HTTP server ---------------------------------------------


class _FakeHttpServer:
    """
    Minimal streamable HTTP MCP server behind httpx.MockTransport. Knows one
    session ("sess-1"); `expire` sets how it answers an unknown session id.
    """

    def __init__(self, expire: str = "aws", status: int = 200):
        self.requests: List[httpx.Request] = []
        self.sessions = {"sess-1"}
        self.expire = expire
        self.status = status
        self._next = 2

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.status != 200:
            return httpx.Response(self.status)
        if request.method == "GET":
            return httpx.Response(405)
        session_id = request.headers.get("mcp-session-id")
        if request.method == "DELETE":
            self.sessions.discard(session_id)
            return httpx.Response(200)
        body = json.loads(request.content)
        method = body.get("method")
        if method == "initialize":
            new_id = f"sess-{self._next}"
            self._next += 1
            self.sessions.add(new_id)
            return self._json(
                body["id"],
                {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "fake", "version": "1"},
                },
                headers={"mcp-session-id": new_id},
            )
        if "id" not in body:
            return httpx.Response(202)
        if session_id not in self.sessions:
            if self.expire == "spec":
                return httpx.Response(404)
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": body["id"],
                    "error": {
                        "code": -30001,
                        "message": "The provided SessionId was not found or has expired",
                    },
                },
            )
        if method == "tools/call":
            return self._json(
                body["id"],
                {
                    "content": [{"type": "text", "text": f"ran in {session_id}"}],
                    "isError": False,
                },
            )
        if method == "tools/list":
            return self._json(body["id"], {"tools": []})
        return httpx.Response(400)

    @staticmethod
    def _json(request_id, result, headers=None) -> httpx.Response:
        return httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": request_id, "result": result},
            headers=headers,
        )

    def methods(self) -> List[str]:
        out = []
        for r in self.requests:
            method = json.loads(r.content).get("method") if r.content else None
            parts = [r.method, method, r.headers.get("mcp-session-id", "-")]
            out.append(" ".join(p for p in parts if p))
        return out


def _http_run(
    fake: _FakeHttpServer, auth: Optional[ResolvedAuth] = None, **kwargs
) -> Dict[str, Any]:
    return run_operation(
        _URL,
        auth or ResolvedAuth(),
        "call_tool",
        tool="aws___run_script",
        arguments={"code": "result = 1"},
        transport=httpx.MockTransport(fake),
        **kwargs,
    )


class TestHttp(TestCase):
    def test_fresh_call_closes_the_session_by_default(self):
        fake = _FakeHttpServer()
        result = _http_run(fake)
        self.assertIsNone(result["session_id"])
        self.assertFalse(result["session_resumed"])
        self.assertEqual(
            [
                "POST initialize -",
                "POST notifications/initialized sess-2",
                "POST tools/call sess-2",
                "DELETE sess-2",
            ],
            [m for m in fake.methods() if not m.startswith("GET")],
        )

    def test_keep_session_returns_it_without_delete(self):
        fake = _FakeHttpServer()
        result = _http_run(fake, keep_session=True)
        self.assertEqual("sess-2", result["session_id"])
        self.assertEqual("2025-06-18", result["protocol_version"])
        self.assertNotIn("DELETE", " ".join(fake.methods()))

    def test_resume_skips_initialize_and_tools_list(self):
        fake = _FakeHttpServer()
        result = _http_run(
            fake, session_id="sess-1", protocol_version="2025-06-18", keep_session=True
        )
        self.assertEqual("sess-1", result["session_id"])
        self.assertTrue(result["session_resumed"])
        self.assertEqual([{"type": "text", "text": "ran in sess-1"}], result["content"])
        self.assertEqual(["POST tools/call sess-1"], fake.methods())
        self.assertEqual("2025-06-18", fake.requests[0].headers["mcp-protocol-version"])
        self.assertNotIn("initialize_ms", result["timings"])

    def test_expired_session_falls_back_to_a_fresh_one(self):
        for expire in ("aws", "spec"):
            fake = _FakeHttpServer(expire=expire)
            result = _http_run(
                fake,
                session_id="gone",
                protocol_version="2025-06-18",
                keep_session=True,
            )
            self.assertFalse(result["session_resumed"], expire)
            self.assertEqual("sess-2", result["session_id"], expire)
            self.assertEqual(
                ["POST tools/call gone", "POST initialize -"],
                [m for m in fake.methods() if not m.startswith("GET")][:2],
                expire,
            )

    def test_auth_failure_does_not_retry(self):
        fake = _FakeHttpServer(status=401)
        with self.assertRaises(McpClientError) as ctx:
            _http_run(fake)
        self.assertEqual(McpErrorCode.AUTH_FAILED, ctx.exception.code)
        self.assertEqual(1, len([r for r in fake.requests if r.method == "POST"]))

    def test_404_on_a_fresh_call_is_not_retried(self):
        fake = _FakeHttpServer(status=404)
        with self.assertRaises(McpClientError) as ctx:
            _http_run(fake)
        self.assertEqual(McpErrorCode.SERVER_ERROR, ctx.exception.code)
        self.assertEqual(1, len([r for r in fake.requests if r.method == "POST"]))

    def test_signs_requests_and_sends_meta_and_headers(self):
        fake = _FakeHttpServer()
        auth = ResolvedAuth(
            httpx_auth=SigV4HttpxAuth(
                Credentials("AKIA_TEST", "secret", "token"), region="us-east-1"
            ),
            headers={"X-Extra": "1"},
            meta={"AWS_REGION": "us-east-1"},
            assume_role_ms=7,
        )
        result = _http_run(fake, auth=auth)
        for request in fake.requests:
            self.assertIn("Credential=AKIA_TEST/", request.headers["authorization"])
            self.assertEqual("1", request.headers["x-extra"])
        call = next(
            json.loads(r.content)
            for r in fake.requests
            if r.content and json.loads(r.content).get("method") == "tools/call"
        )
        self.assertEqual({"AWS_REGION": "us-east-1"}, call["params"]["_meta"])
        self.assertEqual(7, result["timings"]["assume_role_ms"])

    def test_unparseable_post_response_fails_fast_with_server_error(self):
        bad_responses = {
            "html_content_type": httpx.Response(
                200, headers={"content-type": "text/html"}, text="<html>hello</html>"
            ),
            "invalid_json_body": httpx.Response(
                200, headers={"content-type": "application/json"}, text="not json"
            ),
        }
        for name, bad in bad_responses.items():

            def handler(request: httpx.Request, bad=bad) -> httpx.Response:
                if request.method == "GET":
                    return httpx.Response(405)
                if request.method == "DELETE":
                    return httpx.Response(200)
                return bad

            with self.subTest(name):
                started = time.monotonic()
                with self.assertRaises(McpClientError) as ctx:
                    run_operation(
                        _URL,
                        ResolvedAuth(),
                        "call_tool",
                        tool="x",
                        transport=httpx.MockTransport(handler),
                        limits=McpLimits(timeout_seconds=5),
                    )
                elapsed = time.monotonic() - started
                self.assertEqual(McpErrorCode.SERVER_ERROR, ctx.exception.code)
                self.assertLess(elapsed, 3)

    def test_resumed_auth_failure_does_not_fall_back(self):
        fake = _FakeHttpServer(status=401)
        with self.assertRaises(McpClientError) as ctx:
            _http_run(fake, session_id="sess-1", protocol_version="2025-06-18")
        self.assertEqual(McpErrorCode.AUTH_FAILED, ctx.exception.code)
        self.assertNotIn("initialize", " ".join(fake.methods()))

    def test_resume_without_keep_session_closes_it(self):
        fake = _FakeHttpServer()
        result = _http_run(fake, session_id="sess-1", protocol_version="2025-06-18")
        self.assertTrue(result["session_resumed"])
        self.assertIsNone(result["session_id"])
        self.assertEqual(
            ["POST tools/call sess-1", "DELETE sess-1"],
            [m for m in fake.methods() if not m.startswith("GET")],
        )

    def test_sdk_debug_logging_is_suppressed(self):
        # assertNoLogs would reset the logger's level and defeat the pin
        records: List[logging.LogRecord] = []
        handler = logging.Handler(logging.DEBUG)
        handler.emit = records.append  # type: ignore[method-assign]
        root = logging.getLogger()
        previous_level = root.level
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)  # debug mode
        try:
            _http_run(_FakeHttpServer())
        finally:
            root.removeHandler(handler)
            root.setLevel(previous_level)
        sdk_records = [r for r in records if r.name == "mcp.client.streamable_http"]
        # the capture works (INFO records are expected) ...
        self.assertTrue([r for r in sdk_records if r.levelno == logging.INFO])
        # ... and nothing below INFO is emitted
        self.assertEqual(
            [],
            [r.getMessage() for r in sdk_records if r.levelno < logging.INFO],
        )

    def test_redirects_are_not_followed(self):
        def redirect(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                307, headers={"location": "https://evil.example.com/mcp"}
            )

        with self.assertRaises(McpClientError) as ctx:
            run_operation(
                _URL,
                ResolvedAuth(),
                "list_tools",
                transport=httpx.MockTransport(redirect),
            )
        self.assertEqual(McpErrorCode.SERVER_ERROR, ctx.exception.code)


# ---- connect-time SSRF guard (YET-3164) ---------------------------------------


class _LoopbackMcpServer:
    """`_FakeHttpServer` served over a real socket on 127.0.0.1. HTTP/1.0, so
    every request opens a new connection (and a new DNS lookup)."""

    def __init__(self) -> None:
        self.fake = _FakeHttpServer()
        self.paths: List[str] = []
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def _handle(self) -> None:
                outer.paths.append(self.path)
                length = int(self.headers.get("content-length") or 0)
                request = httpx.Request(
                    self.command,
                    "http://mcp.test/mcp",
                    headers=dict(self.headers),
                    content=self.rfile.read(length),
                )
                response = outer.fake(request)
                self.send_response(response.status_code)
                for name, value in response.headers.items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(response.content)

            do_GET = do_POST = do_DELETE = _handle

            def log_message(self, *args: Any) -> None:
                pass

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._server.server_address[1]
        threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        ).start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


@contextmanager
def _loopback_allowed() -> Iterator[None]:
    # loopback is blocked by default; the test server lives there
    networks = [n for n in url_safety._DEFAULT_NETWORKS if str(n) != "127.0.0.0/8"]
    with patch.object(url_safety, "_DEFAULT_NETWORKS", networks):
        yield


@contextmanager
def _recording_connects() -> Iterator[List[str]]:
    """Records the address every real TCP connect goes to."""
    calls: List[str] = []
    original = httpcore.AnyIOBackend.connect_tcp

    async def connect_tcp(self: Any, host: str, port: int, *args: Any, **kwargs: Any):
        calls.append(host)
        return await original(self, host, port, *args, **kwargs)

    with patch.object(httpcore.AnyIOBackend, "connect_tcp", connect_tcp):
        yield calls


def _resolver(answers: Dict[str, str], default: str = "169.254.169.254"):
    lookups: List[str] = []

    def getaddrinfo(host: Any, port: Any, *args: Any, **kwargs: Any) -> List[tuple]:
        name = host.decode() if isinstance(host, bytes) else host
        lookups.append(name)
        return [addrinfo(answers.get(name, default), port)]

    return getaddrinfo, lookups


def _list_tools(url: str, timeout: float = 2) -> Dict[str, Any]:
    return run_operation(
        url, ResolvedAuth(), "list_tools", limits=McpLimits(timeout_seconds=timeout)
    )


class TestSsrfGuard(TestCase):
    def setUp(self):
        self.server = _LoopbackMcpServer()
        self.addCleanup(self.server.close)

    def test_direct_connection_goes_through_the_guard(self):
        getaddrinfo, lookups = _resolver({"mcp.test": "127.0.0.1"})
        with isolated_env({}), _loopback_allowed(), patch(
            "socket.getaddrinfo", side_effect=getaddrinfo
        ), _recording_connects() as connects:
            result = _list_tools(f"http://mcp.test:{self.server.port}/mcp")
        self.assertEqual([], result["tools"])
        self.assertIn("mcp.test", lookups)
        self.assertTrue(connects)
        self.assertEqual({"127.0.0.1"}, set(connects))

    def test_forward_proxy_connection_goes_through_the_guard(self):
        # mcp.test itself resolves to the metadata service: only the proxy may
        # be connected to, so success proves the request went through it.
        getaddrinfo, lookups = _resolver({"proxy.test": "127.0.0.1"})
        env = {"HTTP_PROXY": f"http://proxy.test:{self.server.port}"}
        with isolated_env(env), _loopback_allowed(), patch(
            "socket.getaddrinfo", side_effect=getaddrinfo
        ), _recording_connects() as connects:
            result = _list_tools("http://mcp.test/mcp")
        self.assertEqual([], result["tools"])
        self.assertEqual({"proxy.test"}, set(lookups))
        self.assertEqual({"127.0.0.1"}, set(connects))
        self.assertIn("http://mcp.test/mcp", self.server.paths)  # absolute-form

    def test_block_on_a_later_connection_fails_the_call(self):
        # initialize connects; every later connection (notification, GET
        # stream, tools/list, DELETE) resolves to the metadata service. The SDK
        # swallows some of those errors; the call must still fail.
        answers = iter(["127.0.0.1"])

        def getaddrinfo(host: Any, port: Any, *args: Any, **kwargs: Any):
            return [addrinfo(next(answers, "169.254.169.254"), port)]

        with isolated_env({}), _loopback_allowed(), patch(
            "socket.getaddrinfo", side_effect=getaddrinfo
        ), _recording_connects() as connects:
            with self.assertRaises(McpClientError) as ctx:
                _list_tools(f"http://mcp.test:{self.server.port}/mcp")
        self.assertEqual(McpErrorCode.SERVER_NOT_ALLOWED, ctx.exception.code)
        self.assertEqual(["127.0.0.1"], connects)
        self.assertIn("initialize", self.server.fake.methods()[0])

    def test_swallowed_block_fails_an_otherwise_successful_call(self):
        # tools/list succeeds; the connection that closes the session (DELETE)
        # then resolves to the metadata service. The SDK logs and swallows
        # that error, so only the post-call check can fail the operation.
        def getaddrinfo(host: Any, port: Any, *args: Any, **kwargs: Any):
            listed = any("tools/list" in m for m in self.server.fake.methods())
            return [addrinfo("169.254.169.254" if listed else "127.0.0.1", port)]

        with isolated_env({}), _loopback_allowed(), patch(
            "socket.getaddrinfo", side_effect=getaddrinfo
        ):
            with self.assertRaises(McpClientError) as ctx:
                _list_tools(f"http://mcp.test:{self.server.port}/mcp")
        self.assertEqual(McpErrorCode.SERVER_NOT_ALLOWED, ctx.exception.code)
        # the call itself went through; the DELETE never reached the server
        methods = " ".join(self.server.fake.methods())
        self.assertIn("tools/list", methods)
        self.assertNotIn("DELETE", methods)


class TestSsrfGuardWiring(TestCase):
    """Structural canary: with no transport injected, every transport the
    client can route through (default + env-proxy mounts) carries the guard.
    Catches an httpx upgrade changing how env proxies become mounts."""

    _ENVS = (
        {},
        {"HTTPS_PROXY": "http://proxy.test:3128"},
        {"https_proxy": "http://proxy.test:3128"},
        {"HTTP_PROXY": "http://proxy.test:3128"},
        {"ALL_PROXY": "http://proxy.test:3128"},
        {"HTTPS_PROXY": "http://proxy.test:3128", "NO_PROXY": "mcp.test"},
        {"HTTPS_PROXY": "http://proxy.test:3128", "NO_PROXY": "*"},
        {
            "HTTP_PROXY": "http://proxy.test:3128",
            "HTTPS_PROXY": "http://proxy.test:3129",
            "ALL_PROXY": "http://proxy.test:3130",
            "NO_PROXY": "internal.test,10.0.0.0/8",
        },
    )

    def test_every_transport_is_guarded(self):
        for env in self._ENVS:
            with self.subTest(env=env):
                hardened: List[Any] = []

                def capture(client: httpx.AsyncClient, backend: Any) -> None:
                    hardened.append((client, backend))
                    harden_async_client(client, backend)

                connect = AsyncMock(side_effect=httpcore.ConnectError("unreachable"))
                with isolated_env(env), patch(
                    "apollo.integrations.mcp.session.harden_async_client", capture
                ), patch(
                    "socket.getaddrinfo", return_value=[addrinfo("93.184.216.34")]
                ), patch.object(
                    httpcore.AnyIOBackend, "connect_tcp", connect
                ):
                    with self.assertRaises(McpClientError):
                        _list_tools("https://mcp.test/mcp", timeout=1)
                    plain = httpx.AsyncClient()

                self.assertEqual(1, len(hardened))
                client, backend = hardened[0]
                self.assertEqual(set(plain._mounts), set(client._mounts))
                transports = [client._transport]
                transports.extend(t for t in client._mounts.values() if t is not None)
                for transport in transports:
                    self.assertIs(backend, transport._pool._network_backend)
                # no connection ever bypassed the guard: the inner backend only
                # sees IP literals
                self.assertTrue(connect.called)
                for call in connect.call_args_list:
                    self.assertEqual("93.184.216.34", call.args[0])

    def test_unrecognised_transport_fails_closed(self):
        mock = httpx.MockTransport(lambda request: httpx.Response(500))
        with isolated_env({"HTTPS_PROXY": "http://proxy.test:3128"}), patch.object(
            httpx.AsyncClient, "_init_proxy_transport", return_value=mock
        ):
            with self.assertRaises(McpClientError) as ctx:
                _list_tools("https://mcp.test/mcp", timeout=1)
        self.assertEqual(McpErrorCode.SERVER_NOT_ALLOWED, ctx.exception.code)
        self.assertIn("unsupported httpx transport", str(ctx.exception))
