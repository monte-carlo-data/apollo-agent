import os
from unittest import TestCase
from unittest.mock import patch

from apollo.integrations.mcp.allowlist import (
    DEFAULT_ALLOWED_HOST_PATTERNS,
    MCP_ALLOWED_HOSTS_ENV_VAR,
    check_server_url,
    get_allowed_host_patterns,
    host_matches,
)
from apollo.integrations.mcp.errors import McpClientError, McpErrorCode


class TestHostMatches(TestCase):
    def test_regional_aws_endpoint_allowed_by_default(self):
        for host in ("aws-mcp.us-east-1.api.aws", "AWS-MCP.eu-west-1.api.aws."):
            self.assertTrue(host_matches(host, DEFAULT_ALLOWED_HOST_PATTERNS), host)

    def test_wildcard_matches_exactly_one_label(self):
        for host in (
            "aws-mcp.evil.com.api.aws",
            "aws-mcp.us-east-1.api.aws.evil.com",
            "evil.aws-mcp.us-east-1.api.aws",
            "aws-mcp..api.aws",
            "aws-mcp.api.aws",
        ):
            self.assertFalse(host_matches(host, ["aws-mcp.*.api.aws"]), host)

    def test_exact_and_partial_label_patterns(self):
        self.assertTrue(host_matches("mcp.example.com", ["mcp.example.com"]))
        self.assertTrue(host_matches("mcp-1.example.com", ["mcp-*.example.com"]))
        self.assertFalse(host_matches("mcp.example.com.evil", ["mcp.example.com"]))
        self.assertFalse(host_matches("", ["*"]))


class TestAllowedHostPatterns(TestCase):
    @patch.dict(os.environ, {}, clear=True)
    def test_default_only(self):
        self.assertEqual(
            list(DEFAULT_ALLOWED_HOST_PATTERNS), get_allowed_host_patterns()
        )

    @patch.dict(
        os.environ, {MCP_ALLOWED_HOSTS_ENV_VAR: " mcp.example.com, ,*.corp.internal "}
    )
    def test_env_var_extends_default(self):
        self.assertEqual(
            list(DEFAULT_ALLOWED_HOST_PATTERNS)
            + ["mcp.example.com", "*.corp.internal"],
            get_allowed_host_patterns(),
        )


class TestCheckServerUrl(TestCase):
    _PATTERNS = ["aws-mcp.*.api.aws", "mcp.example.com"]

    def test_returns_host_and_port(self):
        self.assertEqual(
            ("aws-mcp.us-east-1.api.aws", 443),
            check_server_url("https://aws-mcp.us-east-1.api.aws/mcp", self._PATTERNS),
        )
        self.assertEqual(
            ("mcp.example.com", 8443),
            check_server_url("https://mcp.example.com:8443/mcp", self._PATTERNS),
        )

    def test_rejects_unsafe_urls(self):
        for url in (
            "http://aws-mcp.us-east-1.api.aws/mcp",
            "https://user:pw@aws-mcp.us-east-1.api.aws/mcp",
            "https://other.example.com/mcp",
            "https:///mcp",
            "",
        ):
            with self.assertRaises(McpClientError, msg=url) as ctx:
                check_server_url(url, self._PATTERNS)
            self.assertEqual(McpErrorCode.SERVER_NOT_ALLOWED, ctx.exception.code)
            self.assertNotIn("pw", str(ctx.exception))
