import logging
from typing import Any, Dict, Optional

from apollo.common.agent.models import AgentOperation
from apollo.common.agent.redact import AgentRedactUtilities
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.http.url_safety import HttpClientError, assert_safe_destination
from apollo.integrations.mcp.allowlist import (
    check_server_url,
    get_allowed_host_patterns,
)
from apollo.integrations.mcp.auth import resolve_auth
from apollo.integrations.mcp.errors import McpClientError, McpErrorCode
from apollo.integrations.mcp.session import McpLimits, run_operation

_logger = logging.getLogger(__name__)

_TRANSPORT_STREAMABLE_HTTP = "streamable_http"

# Tool arguments carry model-written code (aws___run_script) and must not land
# in agent logs; positional args would carry the same values.
_REDACTED_ATTRIBUTES = ["arguments", "args", "kwargs"]


class McpProxyClient(BaseProxyClient):
    """
    Generic MCP client: connects to a remote MCP server (streamable HTTP) and runs
    `tools/list` or `tools/call`. The agent is protocol only; server conventions
    live with the caller.

    credentials["connect_args"]:
        server: {"url", "transport", "auth": {"type", ...}} from the server
            registration
        plus secrets the auth type needs (e.g. `header_value`), resolved from the
        customer's secret store

    The client keeps only the parsed configuration: assumed-role credentials are
    obtained per call and each call opens its own connection, so a cached client
    holds no credentials or live sessions. Results are capped in the agent and
    never written to storage for a pre-signed URL.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        self._connect_args: Dict[str, Any] = (credentials or {}).get(
            "connect_args"
        ) or {}
        server = self._connect_args.get("server") or {}
        self._url: str = server.get("url") or ""
        self._transport: str = server.get("transport") or _TRANSPORT_STREAMABLE_HTTP
        self._auth_config: Dict[str, Any] = server.get("auth") or {}

    @property
    def wrapped_client(self):
        return None

    @property
    def allows_result_location(self) -> bool:
        return False

    def list_tools(
        self,
        limits: Optional[Dict] = None,
        session_id: Optional[str] = None,
        protocol_version: Optional[str] = None,
        keep_session: bool = False,
    ) -> Dict:
        return self._run(
            "list_tools",
            limits=limits,
            session_id=session_id,
            protocol_version=protocol_version,
            keep_session=keep_session,
        )

    def call_tool(
        self,
        tool: str,
        arguments: Optional[Dict] = None,
        limits: Optional[Dict] = None,
        session_id: Optional[str] = None,
        protocol_version: Optional[str] = None,
        keep_session: bool = False,
    ) -> Dict:
        return self._run(
            "call_tool",
            tool=tool,
            arguments=arguments,
            limits=limits,
            session_id=session_id,
            protocol_version=protocol_version,
            keep_session=keep_session,
        )

    def _run(self, operation: str, limits: Optional[Dict], **kwargs: Any) -> Dict:
        if self._transport != _TRANSPORT_STREAMABLE_HTTP:
            raise McpClientError(
                McpErrorCode.BAD_REQUEST,
                f"Unsupported MCP transport: {self._transport}",
            )
        host, port = check_server_url(self._url, get_allowed_host_patterns())
        try:
            # checked on every call, not once per cached client; the MCP SDK uses
            # httpx, which doesn't go through the urllib3 hook that enforces
            # MCD_HTTP_BLOCKED_CIDRS
            assert_safe_destination(host, port)
        except HttpClientError as exc:
            raise McpClientError(McpErrorCode.SERVER_NOT_ALLOWED, str(exc)) from exc

        result = run_operation(
            self._url,
            resolve_auth(self._auth_config, self._connect_args, host),
            operation,
            limits=McpLimits.from_dict(limits),
            **kwargs,
        )
        _logger.info(
            f"MCP {operation} completed",
            extra={
                "mcp_host": host,
                "mcp_session_resumed": result.get("session_resumed"),
                "mcp_truncated": result.get("truncated"),
                "mcp_timings": result.get("timings"),
            },
        )
        return result

    def get_error_type(self, error: Exception) -> Optional[str]:
        return error.code if isinstance(error, McpClientError) else None

    def log_payload(self, request: AgentOperation) -> Dict:
        payload = super().log_payload(request)
        return AgentRedactUtilities.redact_attributes(payload, _REDACTED_ATTRIBUTES)
