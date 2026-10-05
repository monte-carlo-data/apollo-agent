"""
MCP client protocol core (K2-1343 spike prototype).

Deliberately free of ``apollo`` imports so the spike harness Lambda can vendor
this module next to the ``mcp`` SDK without the rest of the agent.

The agent is protocol only: it connects to a server whose URL and auth come
from the registration, runs ``tools/list`` or ``tools/call``, caps the result
and reports timings. Server conventions (AWS ``api_calls`` checks, task polling,
tool allowlists) belong to monolith server profiles, not here.
"""

import fnmatch
import json
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Dict, Generator, List, Optional, Tuple
from urllib.parse import urlparse

import anyio
import boto3
import httpx
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

# Regional AWS MCP Server endpoints, e.g. https://aws-mcp.us-east-1.api.aws/mcp
DEFAULT_ALLOWED_HOST_PATTERNS = ("aws-mcp.*.api.aws",)
AWS_MCP_SIGNING_SERVICE = "aws-mcp"
DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_MAX_RESULT_BYTES = 200_000

AUTH_NONE = "none"
AUTH_SECRET_HEADER = "secret_header"
AUTH_AWS_SIGV4 = "aws_sigv4"

# Headers a registration may never set through secret_header: SigV4 or the
# transport own them.
_RESERVED_HEADERS = frozenset(
    {"host", "authorization", "x-amz-date", "x-amz-security-token", "mcp-session-id"}
)


class McpClientError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def host_matches(host: str, patterns: List[str]) -> bool:
    host = host.lower().rstrip(".")
    # fnmatch "*" also matches dots; a pattern segment must not span labels,
    # otherwise "aws-mcp.*.api.aws" would accept "aws-mcp.evil.com.api.aws".
    for pattern in patterns:
        pattern = pattern.lower().strip()
        if not pattern:
            continue
        host_labels = host.split(".")
        pattern_labels = pattern.split(".")
        if len(host_labels) == len(pattern_labels) and all(
            fnmatch.fnmatchcase(h, p) for h, p in zip(host_labels, pattern_labels)
        ):
            return True
    return False


def check_server_url(url: str, allowed_host_patterns: List[str]) -> Tuple[str, int]:
    """Validate the registered server URL against the allowlist; returns (host, port)."""
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise McpClientError("server_not_allowed", "MCP server URL must use https")
    if parsed.username or parsed.password:
        raise McpClientError(
            "server_not_allowed", "MCP server URL must not embed credentials"
        )
    host = parsed.hostname or ""
    if not host_matches(host, allowed_host_patterns):
        raise McpClientError(
            "server_not_allowed", f"MCP server host not allowed: {host}"
        )
    return host, parsed.port or 443


class SigV4HttpxAuth(httpx.Auth):
    """Signs every MCP HTTP request with SigV4 (same approach as mcp-proxy-for-aws)."""

    requires_request_body = True

    def __init__(self, credentials: Credentials, region: str, service: str):
        self._signer = SigV4Auth(credentials, service, region)

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        headers = dict(request.headers)
        # not part of the server-side canonical request; signing it breaks the signature
        headers.pop("connection", None)
        aws_request = AWSRequest(
            method=request.method,
            url=str(request.url),
            data=request.content,
            headers=headers,
        )
        self._signer.add_auth(aws_request)
        request.headers.update(dict(aws_request.headers))
        yield request


def assume_role(
    role_arn: str,
    external_id: Optional[str] = None,
    sts_client: Any = None,
) -> Credentials:
    """
    Assume the dedicated narrow role. There is intentionally no fallback to the
    ambient (execution-role) credentials: the execution role can assume every
    MonteCarloData-tagged role and read the agent bucket, so a model-written
    script must never run with it.
    """
    if not role_arn:
        raise McpClientError("auth_config", "aws_sigv4 requires assumable_role")
    params: Dict[str, Any] = {
        "RoleArn": role_arn,
        "RoleSessionName": f"mcd_mcp_{int(time.time())}",
        "DurationSeconds": 3600,
    }
    if external_id:
        params["ExternalId"] = external_id
    sts = sts_client or boto3.client("sts")
    creds = sts.assume_role(**params)["Credentials"]
    return Credentials(
        creds["AccessKeyId"], creds["SecretAccessKey"], creds["SessionToken"]
    )


@dataclass
class ResolvedAuth:
    auth: Optional[httpx.Auth] = None
    headers: Dict[str, str] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)
    assume_role_ms: Optional[int] = None


def resolve_auth(
    auth_config: Dict[str, Any],
    secrets: Dict[str, Any],
    sts_client: Any = None,
) -> ResolvedAuth:
    """
    ``auth_config`` comes from the registration (via monolith); ``secrets`` come
    from the agent's credential resolution (customer secret store), never from
    the model.
    """
    auth_type = auth_config.get("type", AUTH_NONE)
    if auth_type == AUTH_NONE:
        return ResolvedAuth()
    if auth_type == AUTH_SECRET_HEADER:
        header_name = auth_config.get("header_name") or "Authorization"
        if header_name.lower() in _RESERVED_HEADERS - {"authorization"}:
            raise McpClientError("auth_config", f"header not allowed: {header_name}")
        value = secrets.get("header_value")
        if not value:
            raise McpClientError("auth_config", "secret_header requires header_value")
        return ResolvedAuth(headers={header_name: value})
    if auth_type == AUTH_AWS_SIGV4:
        region = auth_config.get("region")
        if not region:
            raise McpClientError("auth_config", "aws_sigv4 requires region")
        start = time.perf_counter()
        credentials = assume_role(
            auth_config.get("assumable_role", ""),
            external_id=secrets.get("external_id") or auth_config.get("external_id"),
            sts_client=sts_client,
        )
        return ResolvedAuth(
            auth=SigV4HttpxAuth(
                credentials,
                region=region,
                service=auth_config.get("service") or AWS_MCP_SIGNING_SERVICE,
            ),
            # mcp-proxy-for-aws sends AWS_REGION in _meta; the AWS MCP Server
            # uses it as the default region for call_boto3.
            meta={"AWS_REGION": region},
            assume_role_ms=_ms_since(start),
        )
    raise McpClientError("auth_config", f"unsupported auth type: {auth_type}")


def _ms_since(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)


def cap_result(result: Dict[str, Any], max_result_bytes: int) -> Dict[str, Any]:
    """
    Cap the serialized result in the agent so it never spills to the S3
    presigned-URL path. Text blocks are cut from the end; anything that still
    does not fit is dropped.
    """
    if len(json.dumps(result, default=str).encode()) <= max_result_bytes:
        return result
    result["truncated"] = True
    result["structured_content"] = None
    budget = max_result_bytes - len(json.dumps({**result, "content": []}).encode())
    capped: List[Dict[str, Any]] = []
    for block in result.get("content") or []:
        size = len(json.dumps(block, default=str).encode())
        if size <= budget:
            capped.append(block)
            budget -= size
        elif block.get("type") == "text" and budget > 200:
            text = block.get("text", "")
            # leave room for JSON escaping overhead
            keep = max(0, budget - 200) // 2
            capped.append({**block, "text": text[:keep]})
            budget = 0
        if budget <= 0:
            break
    result["content"] = capped
    return result


async def _run(
    url: str,
    resolved: ResolvedAuth,
    operation: str,
    tool: Optional[str],
    arguments: Optional[Dict[str, Any]],
    timeout_seconds: float,
    max_result_bytes: int,
) -> Dict[str, Any]:
    timings: Dict[str, Any] = {"assume_role_ms": resolved.assume_role_ms}
    start = time.perf_counter()
    with anyio.fail_after(timeout_seconds):
        async with streamablehttp_client(
            url,
            headers=resolved.headers or None,
            auth=resolved.auth,
            timeout=timeout_seconds,
            sse_read_timeout=timeout_seconds,
        ) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                timings["initialize_ms"] = _ms_since(start)
                op_start = time.perf_counter()
                if operation == "list_tools":
                    listed = await session.list_tools()
                    result: Dict[str, Any] = {
                        "tools": [
                            t.model_dump(mode="json", exclude_none=True)
                            for t in listed.tools
                        ]
                    }
                else:
                    called = await session.call_tool(
                        tool or "",
                        arguments or {},
                        read_timeout_seconds=timedelta(seconds=timeout_seconds),
                        meta=resolved.meta or None,
                    )
                    result = {
                        "content": [
                            c.model_dump(mode="json", exclude_none=True)
                            for c in called.content
                        ],
                        "is_error": bool(called.isError),
                        "structured_content": called.structuredContent,
                        "truncated": False,
                    }
                timings["operation_ms"] = _ms_since(op_start)
    timings["total_ms"] = _ms_since(start)
    if operation != "list_tools":
        result = cap_result(result, max_result_bytes)
    result["duration_ms"] = timings["total_ms"]
    result["timings"] = timings
    return result


def run_mcp_operation(
    url: str,
    resolved: ResolvedAuth,
    operation: str,
    tool: Optional[str] = None,
    arguments: Optional[Dict[str, Any]] = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    max_result_bytes: int = DEFAULT_MAX_RESULT_BYTES,
) -> Dict[str, Any]:
    """
    Sync entry point. Agent request threads (gunicorn gthread, apig_wsgi on
    Lambda, Azure activities) have no running event loop, so each call gets its
    own short-lived loop and MCP session.
    """
    if operation not in ("list_tools", "call_tool"):
        raise McpClientError("bad_request", f"unsupported operation: {operation}")
    if operation == "call_tool" and not tool:
        raise McpClientError("bad_request", "call_tool requires tool")
    try:
        return anyio.run(
            _run,
            url,
            resolved,
            operation,
            tool,
            arguments,
            timeout_seconds,
            max_result_bytes,
        )
    except TimeoutError as exc:
        raise McpClientError(
            "agent_timeout", f"MCP {operation} exceeded {timeout_seconds}s"
        ) from exc
