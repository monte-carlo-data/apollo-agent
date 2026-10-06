import asyncio
from unittest import TestCase

import httpx
from mcp.shared.exceptions import McpError
from mcp.types import ErrorData

from apollo.integrations.mcp.errors import (
    McpClientError,
    McpErrorCode,
    map_exception,
)


def _http_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://aws-mcp.us-east-1.api.aws/mcp")
    return httpx.HTTPStatusError(
        "boom", request=request, response=httpx.Response(status, request=request)
    )


def _mcp_error(code: int, message: str) -> McpError:
    return McpError(ErrorData(code=code, message=message))


class TestMapException(TestCase):
    def test_client_error_passes_through(self):
        error = McpClientError(McpErrorCode.BAD_REQUEST, "nope")
        self.assertIs(error, map_exception(error))

    def test_timeouts(self):
        for exc in (TimeoutError(), httpx.ReadTimeout("slow")):
            self.assertEqual(McpErrorCode.AGENT_TIMEOUT, map_exception(exc).code)

    def test_http_status(self):
        self.assertEqual(McpErrorCode.AUTH_FAILED, map_exception(_http_error(401)).code)
        self.assertEqual(McpErrorCode.AUTH_FAILED, map_exception(_http_error(403)).code)
        self.assertEqual(
            McpErrorCode.SERVER_ERROR, map_exception(_http_error(502)).code
        )

    def test_connection_errors(self):
        self.assertEqual(
            McpErrorCode.CONNECTION_ERROR,
            map_exception(httpx.ConnectError("refused")).code,
        )

    def test_expired_session_only_when_resuming(self):
        aws_expired = _mcp_error(
            -30001, "The provided SessionId was not found or has expired"
        )
        spec_expired = _mcp_error(32600, "Session terminated")
        for exc in (aws_expired, spec_expired, _http_error(404)):
            self.assertEqual(
                McpErrorCode.SESSION_EXPIRED, map_exception(exc, resumed=True).code
            )
            self.assertNotEqual(
                McpErrorCode.SESSION_EXPIRED, map_exception(exc, resumed=False).code
            )

    def test_other_mcp_errors_are_server_errors(self):
        mapped = map_exception(_mcp_error(-32601, "Method not found"))
        self.assertEqual(McpErrorCode.SERVER_ERROR, mapped.code)
        self.assertIn("Method not found", str(mapped))

    def test_unwraps_exception_groups(self):
        group = BaseExceptionGroup(
            "unhandled errors in a TaskGroup",
            [ExceptionGroup("inner", [_http_error(401)])],
        )
        mapped = map_exception(group)
        self.assertEqual(McpErrorCode.AUTH_FAILED, mapped.code)
        self.assertNotIn("TaskGroup", str(mapped))

    def test_group_prefers_a_meaningful_leaf_over_cancellation(self):
        cancelled = asyncio.CancelledError()
        group = BaseExceptionGroup("x", [cancelled, httpx.ConnectError("refused")])
        self.assertEqual(McpErrorCode.CONNECTION_ERROR, map_exception(group).code)

    def test_unknown_errors(self):
        mapped = map_exception(ValueError("bad"))
        self.assertEqual(McpErrorCode.INTERNAL_ERROR, mapped.code)
