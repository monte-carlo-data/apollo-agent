import fnmatch
import os
import re
from typing import List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from apollo.integrations.mcp.errors import McpClientError, McpErrorCode

# Comma-separated extra host patterns, added to the defaults. "*" matches exactly
# one DNS label. Settable through MCD_ADDITIONAL_ENV_VARS, so no new
# CloudFormation/Terraform parameter is needed.
MCP_ALLOWED_HOSTS_ENV_VAR = "MCD_MCP_ALLOWED_HOSTS"

# Regional AWS MCP Server endpoints, e.g. https://aws-mcp.us-east-1.api.aws/mcp
DEFAULT_ALLOWED_HOST_PATTERNS = ("aws-mcp.*.api.aws",)
_AWS_MCP_HOST = re.compile(r"^aws-mcp\.([a-z0-9-]+)\.api\.aws$")


def aws_mcp_region(host: str) -> Optional[str]:
    """:return: the region of an `aws-mcp.<region>.api.aws` host, None for any other."""
    match = _AWS_MCP_HOST.match(host.lower().rstrip("."))
    return match.group(1) if match else None


def get_allowed_host_patterns() -> List[str]:
    raw = os.getenv(MCP_ALLOWED_HOSTS_ENV_VAR, "")
    return list(DEFAULT_ALLOWED_HOST_PATTERNS) + [
        pattern.strip() for pattern in raw.split(",") if pattern.strip()
    ]


def host_matches(host: str, patterns: Sequence[str]) -> bool:
    host_labels = host.lower().rstrip(".").split(".")
    if not all(host_labels):
        return False
    for pattern in patterns:
        pattern_labels = pattern.lower().strip().split(".")
        # match label by label: fnmatch's "*" would otherwise also match dots,
        # letting "aws-mcp.*.api.aws" accept "aws-mcp.evil.com.api.aws"
        if len(pattern_labels) == len(host_labels) and all(
            fnmatch.fnmatchcase(h, p) for h, p in zip(host_labels, pattern_labels)
        ):
            return True
    return False


def check_server_url(url: str, allowed_host_patterns: Sequence[str]) -> Tuple[str, int]:
    """
    Validates a registered MCP server URL against the allowlist.
    :return: the URL's host and port, for the caller's SSRF check.
    """
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise McpClientError(
            McpErrorCode.SERVER_NOT_ALLOWED, "MCP server URL must use https"
        )
    if parsed.username or parsed.password:
        raise McpClientError(
            McpErrorCode.SERVER_NOT_ALLOWED, "MCP server URL must not embed credentials"
        )
    host = parsed.hostname or ""
    if not host_matches(host, allowed_host_patterns):
        raise McpClientError(
            McpErrorCode.SERVER_NOT_ALLOWED,
            f"MCP server host is not allowed: {host or '<none>'}. "
            f"Add it to {MCP_ALLOWED_HOSTS_ENV_VAR} to allow it.",
        )
    return host, parsed.port or 443
