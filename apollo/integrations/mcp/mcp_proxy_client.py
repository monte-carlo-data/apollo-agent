import os
from typing import Any, Dict, List, Optional

from apollo.common.agent.models import AgentOperation
from apollo.common.agent.redact import AgentRedactUtilities
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.http.url_safety import assert_safe_destination
from apollo.integrations.mcp.core import (
    DEFAULT_ALLOWED_HOST_PATTERNS,
    DEFAULT_MAX_RESULT_BYTES,
    DEFAULT_TIMEOUT_SECONDS,
    check_server_url,
    resolve_auth,
    run_mcp_operation,
)

# Comma-separated extra host patterns (one label per "*"), added to the AWS MCP
# Server regional endpoints. Settable through MCD_ADDITIONAL_ENV_VARS, so no new
# CloudFormation/Terraform parameter is needed.
MCP_ALLOWED_HOSTS_ENV_VAR = "MCD_MCP_ALLOWED_HOSTS"

# Model-written code and tool arguments must not land in agent logs.
_MCP_REDACTED_ATTRIBUTES = ["arguments", "args", "kwargs"]


def get_allowed_host_patterns() -> List[str]:
    raw = os.getenv(MCP_ALLOWED_HOSTS_ENV_VAR, "")
    return list(DEFAULT_ALLOWED_HOST_PATTERNS) + [
        p.strip() for p in raw.split(",") if p.strip()
    ]


class McpProxyClient(BaseProxyClient):
    """
    Generic MCP client (K2-1343 spike prototype).

    credentials["connect_args"]:
        server: {"url", "transport", "auth": {"type", ...}} from the registration
        header_value / external_id: secrets resolved from the customer secret store

    Operations: ``list_tools()`` and ``call_tool(tool, arguments, limits)``.
    Each call opens and closes its own MCP session, so caching this client only
    caches the parsed config, never assumed-role credentials.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        connect_args = (credentials or {}).get("connect_args") or {}
        server = connect_args.get("server") or {}
        self._url: str = server.get("url", "")
        self._auth_config: Dict[str, Any] = server.get("auth") or {}
        transport = server.get("transport", "streamable_http")
        if transport != "streamable_http":
            raise ValueError(f"unsupported MCP transport: {transport}")
        self._secrets = {
            k: connect_args[k]
            for k in ("header_value", "external_id")
            if k in connect_args
        }
        host, port = check_server_url(self._url, get_allowed_host_patterns())
        # httpx does not go through the urllib3 hook that enforces the block list
        assert_safe_destination(host, port)

    @property
    def wrapped_client(self):
        return None

    def list_tools(self) -> Dict:
        return run_mcp_operation(
            self._url, resolve_auth(self._auth_config, self._secrets), "list_tools"
        )

    def call_tool(
        self,
        tool: str,
        arguments: Optional[Dict] = None,
        limits: Optional[Dict] = None,
    ) -> Dict:
        limits = limits or {}
        return run_mcp_operation(
            self._url,
            resolve_auth(self._auth_config, self._secrets),
            "call_tool",
            tool=tool,
            arguments=arguments,
            timeout_seconds=float(
                limits.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
            ),
            max_result_bytes=int(
                limits.get("max_result_bytes", DEFAULT_MAX_RESULT_BYTES)
            ),
        )

    def log_payload(self, operation: AgentOperation) -> Dict:
        payload = super().log_payload(operation)
        return AgentRedactUtilities.redact_attributes(payload, _MCP_REDACTED_ATTRIBUTES)
