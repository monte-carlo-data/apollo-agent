"""
Runs one MCP operation (`tools/list` or `tools/call`) against a remote server
over streamable HTTP. Each call runs its own short-lived event loop (agent
request threads, Lambda and Azure activities have none), so nothing async
outlives a call; session reuse is native MCP (see `run_operation`).
"""

import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

import anyio
import httpx
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp import ClientSession, types
from mcp.client.session import MessageHandlerFnT
from mcp.shared.exceptions import McpError
from mcp.client.streamable_http import StreamableHTTPTransport
from mcp.shared.message import SessionMessage
from mcp.shared.session import RequestResponder

from apollo.integrations.mcp.auth import ResolvedAuth
from apollo.integrations.mcp.errors import McpClientError, McpErrorCode, map_exception
from apollo.integrations.mcp.results import (
    cap_call_result,
    cap_tools_result,
    convert_call_result,
    convert_tools,
)

# the SDK logs full JSON-RPC messages (tool arguments and results) at DEBUG,
# which would bypass log_payload redaction when an entry point enables debug mode
logging.getLogger("mcp.client.streamable_http").setLevel(logging.INFO)

OPERATION_LIST_TOOLS = "list_tools"
OPERATION_CALL_TOOL = "call_tool"

DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_MAX_RESULT_BYTES = 200_000
_TIMEOUT_RANGE = (1.0, 300.0)
_MAX_RESULT_BYTES_RANGE = (1_000, 1_000_000)
# room left for session_id, protocol_version, timings etc., which are added
# after the content is capped, so the whole result stays under max_result_bytes
_METADATA_RESERVE_BYTES = 512
# a misbehaving server could hand out cursors forever
_MAX_LIST_PAGES = 50

ReadStream = MemoryObjectReceiveStream[SessionMessage | Exception]
WriteStream = MemoryObjectSendStream[SessionMessage]
Streams = Tuple[ReadStream, WriteStream, Callable[[], Optional[str]]]
# (client, url, session_id, protocol_version, terminate_on_close)
StreamsFactory = Callable[
    [httpx.AsyncClient, str, Optional[str], Optional[str], bool],
    AsyncContextManager[Streams],
]


@dataclass(frozen=True)
class McpLimits:
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    max_result_bytes: int = DEFAULT_MAX_RESULT_BYTES

    @classmethod
    def from_dict(cls, limits: Optional[Dict[str, Any]]) -> "McpLimits":
        if limits is None:
            return cls()
        if not isinstance(limits, dict):
            raise McpClientError(McpErrorCode.BAD_REQUEST, "limits must be an object")
        try:
            timeout = float(limits.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS))
            max_bytes = int(limits.get("max_result_bytes", DEFAULT_MAX_RESULT_BYTES))
        except (TypeError, ValueError) as exc:
            raise McpClientError(
                McpErrorCode.BAD_REQUEST, f"Invalid limits: {exc}"
            ) from exc
        return cls(
            timeout_seconds=min(max(timeout, _TIMEOUT_RANGE[0]), _TIMEOUT_RANGE[1]),
            max_result_bytes=min(
                max(max_bytes, _MAX_RESULT_BYTES_RANGE[0]), _MAX_RESULT_BYTES_RANGE[1]
            ),
        )


def run_operation(
    url: str,
    auth: ResolvedAuth,
    operation: str,
    *,
    tool: Optional[str] = None,
    arguments: Optional[Dict[str, Any]] = None,
    limits: Optional[McpLimits] = None,
    session_id: Optional[str] = None,
    protocol_version: Optional[str] = None,
    keep_session: bool = False,
    transport: Optional[httpx.AsyncBaseTransport] = None,
    streams_factory: Optional[StreamsFactory] = None,
) -> Dict[str, Any]:
    """
    Runs `operation` and returns a JSON-safe, size-capped result. Raises
    `McpClientError` on failure.
    :param session_id: resume this server session instead of starting a new one;
        if the server no longer knows it, a new session is started.
    :param protocol_version: the version negotiated when `session_id` was created.
    :param keep_session: leave the session open and return its id, so a later
        request can resume it; otherwise the session is closed (DELETE).
    :param transport: httpx transport override (tests).
    :param streams_factory: MCP message streams override (tests).
    """
    if operation not in (OPERATION_LIST_TOOLS, OPERATION_CALL_TOOL):
        raise McpClientError(
            McpErrorCode.BAD_REQUEST, f"Unsupported MCP operation: {operation}"
        )
    if operation == OPERATION_CALL_TOOL and not tool:
        raise McpClientError(McpErrorCode.BAD_REQUEST, "call_tool requires a tool")
    run = _Run(
        url=url,
        auth=auth,
        operation=operation,
        tool=tool,
        arguments=arguments or {},
        limits=limits or McpLimits(),
        keep_session=keep_session,
        transport=transport,
        streams_factory=streams_factory or _streamable_http_streams,
    )
    try:
        return anyio.run(run.execute, session_id, protocol_version)
    except BaseException as exc:
        raise map_exception(exc, resumed=run.resumed) from exc


@dataclass
class _Run:
    url: str
    auth: ResolvedAuth
    operation: str
    tool: Optional[str]
    arguments: Dict[str, Any]
    limits: McpLimits
    keep_session: bool
    transport: Optional[httpx.AsyncBaseTransport]
    streams_factory: StreamsFactory
    resumed: bool = False

    async def execute(
        self, session_id: Optional[str], protocol_version: Optional[str]
    ) -> Dict[str, Any]:
        start = time.perf_counter()
        timings: Dict[str, Any] = {"assume_role_ms": self.auth.assume_role_ms}
        with anyio.fail_after(self.limits.timeout_seconds):
            result: Optional[Dict[str, Any]] = None
            if session_id:
                self.resumed = True
                try:
                    result = await self._attempt(
                        session_id, protocol_version, timings, start
                    )
                except Exception as exc:
                    if map_exception(exc, resumed=True).code != (
                        McpErrorCode.SESSION_EXPIRED
                    ):
                        raise
                    # the spec says to start a new session (MUST)
                    self.resumed = False
            if result is None:
                result = await self._attempt(None, None, timings, start)
        result["session_resumed"] = self.resumed
        timings["total_ms"] = _ms_since(start)
        result["timings"] = timings
        result["duration_ms"] = timings["total_ms"]
        return result

    async def _attempt(
        self,
        session_id: Optional[str],
        protocol_version: Optional[str],
        timings: Dict[str, Any],
        start: float,
    ) -> Dict[str, Any]:
        async with httpx.AsyncClient(
            auth=self.auth.httpx_auth,
            headers=self.auth.headers,
            timeout=httpx.Timeout(self.limits.timeout_seconds),
            follow_redirects=False,
            transport=self.transport,
        ) as client:
            async with self.streams_factory(
                client, self.url, session_id, protocol_version, not self.keep_session
            ) as (read, write, get_session_id):
                transport_errors: List[Exception] = []
                try:
                    async with _PassthroughClientSession(
                        read,
                        write,
                        message_handler=_fail_on_transport_error(transport_errors),
                    ) as session:
                        if session_id is None:
                            initialized = await session.initialize()
                            protocol_version = str(initialized.protocolVersion)
                            timings["initialize_ms"] = _ms_since(start)
                        operation_start = time.perf_counter()
                        if self.operation == OPERATION_LIST_TOOLS:
                            result = await self._list_tools(session)
                        else:
                            result = await self._call_tool(session)
                        timings["operation_ms"] = _ms_since(operation_start)
                        result["session_id"] = (
                            get_session_id() if self.keep_session else None
                        )
                        result["protocol_version"] = protocol_version
                        return result
                except McpError:
                    if transport_errors:
                        # the pending request only saw "Connection closed"
                        raise transport_errors[0]
                    raise

    async def _list_tools(self, session: ClientSession) -> Dict[str, Any]:
        tools: List[types.Tool] = []
        cursor: Optional[str] = None
        for _ in range(_MAX_LIST_PAGES):
            page = await session.list_tools(
                params=types.PaginatedRequestParams(cursor=cursor) if cursor else None
            )
            tools.extend(page.tools)
            cursor = page.nextCursor
            if not cursor or len(tools) * 64 > self.limits.max_result_bytes:
                break
        return cap_tools_result(
            {"tools": convert_tools(tools), "truncated": bool(cursor)},
            self._content_budget(),
        )

    async def _call_tool(self, session: ClientSession) -> Dict[str, Any]:
        called = await session.call_tool(
            self.tool or "",
            self.arguments,
            read_timeout_seconds=timedelta(seconds=self.limits.timeout_seconds),
            meta=self.auth.meta or None,
        )
        return cap_call_result(convert_call_result(called), self._content_budget())

    def _content_budget(self) -> int:
        return self.limits.max_result_bytes - _METADATA_RESERVE_BYTES


def _fail_on_transport_error(
    transport_errors: List[Exception],
) -> MessageHandlerFnT:
    """
    Message handler for failures the transport reports by sending an Exception
    into the read stream (unparseable response, unexpected content type); the
    SDK ignores them by default, so the pending request would hang until the
    timeout. Raising ends the session's receive loop (the SDK logs and swallows
    it), which fails pending requests with "Connection closed"; `_attempt`
    swaps that for the recorded exception.
    """

    async def handler(
        message: Union[
            RequestResponder[types.ServerRequest, types.ClientResult],
            types.ServerNotification,
            Exception,
        ],
    ) -> None:
        if isinstance(message, Exception):
            transport_errors.append(message)
            raise message

    return handler


class _PassthroughClientSession(ClientSession):
    """
    The agent passes tool results through without validating structured content
    against the tool's output schema. The SDK's validation sends an extra
    tools/list on every new session (~0.5 s on the AWS MCP Server).
    """

    async def _validate_tool_result(
        self, name: str, result: types.CallToolResult
    ) -> None:
        return None


@asynccontextmanager
async def _streamable_http_streams(
    client: httpx.AsyncClient,
    url: str,
    session_id: Optional[str],
    protocol_version: Optional[str],
    terminate_on_close: bool,
) -> AsyncIterator[Streams]:
    """
    `mcp.client.streamable_http.streamable_http_client` (mcp 1.30), except that
    the transport can start with a known session id and protocol version, which
    the SDK has no parameter for.
    """
    read_writer, read_stream = anyio.create_memory_object_stream[
        SessionMessage | Exception
    ](0)
    write_stream, write_reader = anyio.create_memory_object_stream[SessionMessage](0)
    transport = StreamableHTTPTransport(url)
    transport.session_id = session_id
    transport.protocol_version = protocol_version

    async with anyio.create_task_group() as tg:
        try:

            def start_get_stream() -> None:
                tg.start_soon(transport.handle_get_stream, client, read_writer)

            tg.start_soon(
                transport.post_writer,
                client,
                write_reader,
                read_writer,
                write_stream,
                start_get_stream,
                tg,
            )
            try:
                yield read_stream, write_stream, transport.get_session_id
            finally:
                if transport.session_id and terminate_on_close:
                    await transport.terminate_session(client)
                tg.cancel_scope.cancel()
        finally:
            await read_writer.aclose()
            await write_stream.aclose()


def _ms_since(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)
