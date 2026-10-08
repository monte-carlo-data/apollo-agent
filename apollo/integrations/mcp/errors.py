import asyncio
from typing import Optional

import httpx
from mcp.shared.exceptions import McpError
from pydantic import ValidationError

# JSON-RPC error codes the AWS MCP Server and spec-compliant servers use for a
# session they no longer know: AWS answers HTTP 200 with -30001; the SDK turns a
# spec 404 ("Session terminated") into 32600.
_AWS_SESSION_NOT_FOUND = -30001
_SDK_SESSION_TERMINATED = 32600


class McpErrorCode:
    """Error types reported as `__mcd_error_type__` for MCP operations."""

    AGENT_TIMEOUT = "agent_timeout"
    AUTH_CONFIG = "auth_config"
    AUTH_FAILED = "auth_failed"
    AWS_ACCESS_DENIED = "aws_access_denied"
    BAD_REQUEST = "bad_request"
    CONNECTION_ERROR = "connection_error"
    INTERNAL_ERROR = "internal_error"
    SERVER_ERROR = "server_error"
    SERVER_NOT_ALLOWED = "server_not_allowed"
    SESSION_EXPIRED = "session_expired"


class McpClientError(Exception):
    """An MCP operation failure with a stable code and a short, model-readable message."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def map_exception(exc: BaseException, resumed: bool = False) -> McpClientError:
    """
    Maps whatever an MCP operation raised to an `McpClientError`. anyio task groups
    wrap errors in (nested) exception groups; the first meaningful leaf decides.
    :param resumed: the operation ran on a resumed session, so "unknown session"
        errors mean the session expired rather than a server failure.
    """
    leaf = _first_meaningful_leaf(exc)
    if isinstance(leaf, McpClientError):
        return leaf
    if isinstance(leaf, TimeoutError):
        return McpClientError(McpErrorCode.AGENT_TIMEOUT, "MCP operation timed out")

    mapped = _map_http_error(leaf, resumed) or _map_mcp_error(leaf, resumed)
    if mapped:
        return mapped
    # a response the SDK could not parse (JSONDecodeError is a ValueError); the
    # server is at fault, so never `session_expired`, even when resuming
    if isinstance(leaf, (ValueError, ValidationError)):
        return McpClientError(
            McpErrorCode.SERVER_ERROR,
            f"MCP server sent an invalid response: {type(leaf).__name__}",
        )
    return McpClientError(
        McpErrorCode.INTERNAL_ERROR, f"MCP client error: {type(leaf).__name__}"
    )


def _first_meaningful_leaf(exc: BaseException) -> BaseException:
    if not isinstance(exc, BaseExceptionGroup):
        return exc
    leaves = [_first_meaningful_leaf(e) for e in exc.exceptions]
    for leaf in leaves:
        if not _is_cancellation(leaf):
            return leaf
    return leaves[0]


def _is_cancellation(exc: BaseException) -> bool:
    # anyio runs on asyncio here, so task-group cancellation surfaces as this
    return isinstance(exc, asyncio.CancelledError)


def _map_http_error(exc: BaseException, resumed: bool) -> Optional[McpClientError]:
    if isinstance(exc, httpx.TimeoutException):
        return McpClientError(McpErrorCode.AGENT_TIMEOUT, "MCP operation timed out")
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status in (401, 403):
            return McpClientError(
                McpErrorCode.AUTH_FAILED,
                f"MCP server rejected the credentials ({status})",
            )
        if status == 404 and resumed:
            return _session_expired()
        return McpClientError(
            McpErrorCode.SERVER_ERROR, f"MCP server returned HTTP {status}"
        )
    if isinstance(exc, httpx.TransportError):
        return McpClientError(
            McpErrorCode.CONNECTION_ERROR,
            f"Could not reach the MCP server: {type(exc).__name__}",
        )
    return None


def _map_mcp_error(exc: BaseException, resumed: bool) -> Optional[McpClientError]:
    if not isinstance(exc, McpError):
        return None
    if resumed and exc.error.code in (_AWS_SESSION_NOT_FOUND, _SDK_SESSION_TERMINATED):
        return _session_expired()
    return McpClientError(
        McpErrorCode.SERVER_ERROR, f"MCP server error: {exc.error.message}"
    )


def _session_expired() -> McpClientError:
    return McpClientError(
        McpErrorCode.SESSION_EXPIRED, "MCP session not found or expired"
    )
