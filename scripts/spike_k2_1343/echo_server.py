"""
K2-1343 spike: minimal non-AWS remote MCP server behind a Lambda Function URL.

Stateless streamable HTTP with JSON responses; every request must carry the
static API key in X-Api-Key (the "secret_header" auth type).
"""

import hmac
import os
from typing import Any, Dict

from mangum import Mangum  # type: ignore[import-not-found]  # Lambda-only dependency
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.responses import JSONResponse

_API_KEY = os.environ["ECHO_API_KEY"]


def echo(text: str) -> str:
    """Return the input text."""
    return text


def add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b


class ApiKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        supplied = request.headers.get("x-api-key", "")
        if not hmac.compare_digest(supplied, _API_KEY):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return await call_next(request)


def _build_app() -> Starlette:
    # StreamableHTTPSessionManager.run() is once-per-instance and Mangum runs the
    # lifespan on every invocation, so build a fresh (stateless) app per request.
    server = FastMCP("k2-1343-echo", stateless_http=True, json_response=True)
    # Function URL host is not known up front; DNS-rebinding protection is a
    # local-server concern.
    server.settings.transport_security = None
    for fn in (echo, add):
        server.add_tool(fn, annotations=ToolAnnotations(readOnlyHint=True))
    app = server.streamable_http_app()
    app.add_middleware(ApiKeyMiddleware)
    return app


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    return Mangum(_build_app(), lifespan="auto")(event, context)
