import json
from unittest import TestCase
from unittest.mock import MagicMock, patch

import httpx

from apollo.agent.agent import Agent
from apollo.common.agent.constants import ATTRIBUTE_NAME_ERROR, ATTRIBUTE_NAME_RESULT
from apollo.agent.logging_utils import LoggingUtils
from apollo.common.agent.models import AgentOperation
from apollo.integrations.mcp.core import (
    McpClientError,
    SigV4HttpxAuth,
    assume_role,
    cap_result,
    check_server_url,
    host_matches,
    resolve_auth,
)
from apollo.integrations.mcp.mcp_proxy_client import McpProxyClient

_AWS_URL = "https://aws-mcp.us-east-1.api.aws/mcp"
_ROLE = "arn:aws:iam::123456789012:role/narrow"
_SIGV4_SERVER = {
    "url": _AWS_URL,
    "transport": "streamable_http",
    "auth": {"type": "aws_sigv4", "region": "us-east-1", "assumable_role": _ROLE},
}
_STS_RESPONSE = {
    "Credentials": {
        "AccessKeyId": "AKIA_TEST",
        "SecretAccessKey": "secret",
        "SessionToken": "session-token",
    }
}


class TestMcpHostAllowlist(TestCase):
    def test_regional_aws_endpoint_allowed(self):
        self.assertTrue(
            host_matches("aws-mcp.eu-west-1.api.aws", ["aws-mcp.*.api.aws"])
        )

    def test_wildcard_does_not_span_labels(self):
        self.assertFalse(
            host_matches("aws-mcp.evil.com.api.aws", ["aws-mcp.*.api.aws"])
        )
        self.assertFalse(
            host_matches("aws-mcp.us-east-1.api.aws.evil.com", ["aws-mcp.*.api.aws"])
        )

    def test_rejects_http_userinfo_and_unlisted_hosts(self):
        for url in (
            "http://aws-mcp.us-east-1.api.aws/mcp",
            "https://user:pw@aws-mcp.us-east-1.api.aws/mcp",
            "https://mcp.example.com/mcp",
        ):
            with self.assertRaises(McpClientError, msg=url):
                check_server_url(url, ["aws-mcp.*.api.aws"])

    @patch.dict("os.environ", {"MCD_MCP_ALLOWED_HOSTS": "mcp.example.com"})
    @patch("apollo.integrations.mcp.mcp_proxy_client.assert_safe_destination")
    def test_env_var_extends_allowlist_and_runs_ssrf_check(self, mock_safe):
        McpProxyClient(
            {"connect_args": {"server": {"url": "https://mcp.example.com/mcp"}}}
        )
        mock_safe.assert_called_once_with("mcp.example.com", 443)


class TestMcpAuth(TestCase):
    def test_sigv4_requires_role_and_never_falls_back(self):
        sts = MagicMock()
        with self.assertRaises(McpClientError):
            resolve_auth({"type": "aws_sigv4", "region": "us-east-1"}, {}, sts)
        sts.assume_role.assert_not_called()

    def test_assume_role_passes_external_id(self):
        sts = MagicMock()
        sts.assume_role.return_value = _STS_RESPONSE
        creds = assume_role(_ROLE, external_id="ext", sts_client=sts)
        kwargs = sts.assume_role.call_args.kwargs
        self.assertEqual(_ROLE, kwargs["RoleArn"])
        self.assertEqual("ext", kwargs["ExternalId"])
        self.assertEqual("session-token", creds.token)

    def test_sigv4_auth_signs_request(self):
        sts = MagicMock()
        sts.assume_role.return_value = _STS_RESPONSE
        resolved = resolve_auth(_SIGV4_SERVER["auth"], {}, sts)
        self.assertIsInstance(resolved.auth, SigV4HttpxAuth)
        self.assertEqual({"AWS_REGION": "us-east-1"}, resolved.meta)

        request = httpx.Request("POST", _AWS_URL, json={"jsonrpc": "2.0"})
        signed = next(resolved.auth.auth_flow(request))
        self.assertIn(
            "/us-east-1/aws-mcp/aws4_request", signed.headers["Authorization"]
        )
        self.assertEqual("session-token", signed.headers["X-Amz-Security-Token"])

    def test_secret_header_comes_from_secrets_only(self):
        resolved = resolve_auth(
            {"type": "secret_header", "header_name": "X-Api-Key"},
            {"header_value": "k"},
        )
        self.assertEqual({"X-Api-Key": "k"}, resolved.headers)
        with self.assertRaises(McpClientError):
            resolve_auth({"type": "secret_header"}, {})
        with self.assertRaises(McpClientError):
            resolve_auth(
                {"type": "secret_header", "header_name": "X-Amz-Security-Token"},
                {"header_value": "k"},
            )


class TestMcpResultCap(TestCase):
    def test_under_limit_untouched(self):
        result = {"content": [{"type": "text", "text": "hi"}], "truncated": False}
        self.assertEqual(result, cap_result(dict(result), 1000))

    def test_over_limit_truncates_text(self):
        result = {
            "content": [{"type": "text", "text": "x" * 50_000}],
            "is_error": False,
            "structured_content": {"big": "y" * 50_000},
            "truncated": False,
        }
        capped = cap_result(result, 10_000)
        self.assertTrue(capped["truncated"])
        self.assertIsNone(capped["structured_content"])
        self.assertLessEqual(len(json.dumps(capped).encode()), 10_000)


class TestMcpAgentRoute(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())

    @patch("apollo.integrations.mcp.mcp_proxy_client.assert_safe_destination")
    @patch("apollo.integrations.mcp.mcp_proxy_client.resolve_auth")
    @patch("apollo.integrations.mcp.mcp_proxy_client.run_mcp_operation")
    def test_call_tool_through_execute_operation(self, mock_run, mock_auth, _):
        mock_run.return_value = {"content": [], "is_error": False}
        operation = {
            "trace_id": "t1",
            "skip_cache": True,
            "commands": [
                {
                    "method": "call_tool",
                    "kwargs": {
                        "tool": "aws___run_script",
                        "arguments": {"code": "result={}\nresult"},
                        "limits": {"timeout_seconds": 5, "max_result_bytes": 1000},
                    },
                }
            ],
        }
        response = self._agent.execute_operation(
            "mcp", "call_tool", operation, {"connect_args": {"server": _SIGV4_SERVER}}
        )
        self.assertNotIn(ATTRIBUTE_NAME_ERROR, response.result)
        self.assertEqual(
            {"content": [], "is_error": False}, response.result[ATTRIBUTE_NAME_RESULT]
        )
        kwargs = mock_run.call_args.kwargs
        self.assertEqual("aws___run_script", kwargs["tool"])
        self.assertEqual(5.0, kwargs["timeout_seconds"])
        mock_auth.assert_called_once_with(_SIGV4_SERVER["auth"], {})

    @patch("apollo.integrations.mcp.mcp_proxy_client.assert_safe_destination")
    def test_disallowed_host_is_an_error(self, _):
        response = self._agent.execute_operation(
            "mcp",
            "list_tools",
            {
                "trace_id": "t2",
                "skip_cache": True,
                "commands": [{"method": "list_tools"}],
            },
            {"connect_args": {"server": {"url": "https://evil.example.com/mcp"}}},
        )
        self.assertIn("not allowed", response.result[ATTRIBUTE_NAME_ERROR])

    @patch("apollo.integrations.mcp.mcp_proxy_client.assert_safe_destination")
    def test_log_payload_redacts_tool_arguments(self, _):
        client = McpProxyClient({"connect_args": {"server": _SIGV4_SERVER}})
        operation = AgentOperation.from_dict(
            {
                "trace_id": "t3",
                "commands": [
                    {
                        "method": "call_tool",
                        "kwargs": {"arguments": {"code": "secret()"}},
                    }
                ],
            }
        )
        self.assertNotIn("secret()", json.dumps(client.log_payload(operation)))
