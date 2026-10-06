import json
import logging
from typing import Any, Dict
from unittest import TestCase
from unittest.mock import ANY, Mock, patch

import pytest

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.proxy_client_factory import (
    ProxyClientFactory,
    get_native_connection_types,
)
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_ERROR_TYPE,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_RESULT_LOCATION,
)
from apollo.common.agent.models import AgentCommands
from apollo.integrations.aws.aws_utils import AwsSession
from apollo.integrations.http.url_safety import HttpClientError
from apollo.integrations.mcp.auth import SigV4HttpxAuth
from apollo.integrations.mcp.errors import McpClientError, McpErrorCode
from apollo.integrations.mcp.mcp_proxy_client import McpProxyClient
from apollo.integrations.mcp.session import McpLimits
from tests.test_mcp_session import _memory_server, _memory_streams

_URL = "https://aws-mcp.us-east-1.api.aws/mcp"
_ROLE = "arn:aws:iam::123456789012:role/narrow"
_SCRIPT = 'r = await call_boto3(service_name="logs", operation_name="FilterLogEvents")'
_CREDENTIALS = {
    "connect_args": {
        "server": {
            "url": _URL,
            "transport": "streamable_http",
            "auth": {"type": "aws_sigv4", "assumable_role": _ROLE},
        }
    }
}
_RESULT = {
    "content": [{"type": "text", "text": "ok"}],
    "is_error": False,
    "structured_content": None,
    "truncated": False,
    "session_id": "sess-1",
    "protocol_version": "2025-06-18",
    "session_resumed": False,
    "duration_ms": 10,
    "timings": {"total_ms": 10},
}


def _call_tool_operation(**kwargs: Any) -> Dict[str, Any]:
    return {
        "trace_id": "trace-1",
        "skip_cache": True,
        "commands": [
            {
                "method": "call_tool",
                "kwargs": {
                    "tool": "aws___run_script",
                    "arguments": {"code": _SCRIPT},
                    "limits": {"timeout_seconds": 15, "max_result_bytes": 50_000},
                    "session_id": "sess-1",
                    "protocol_version": "2025-06-18",
                    "keep_session": True,
                },
            }
        ],
        **kwargs,
    }


@patch("apollo.integrations.mcp.mcp_proxy_client.assert_safe_destination")
@patch("apollo.integrations.mcp.auth.assume_role")
@patch("apollo.integrations.mcp.mcp_proxy_client.run_operation")
class TestMcpProxyClientThroughAgent(TestCase):
    def setUp(self):
        self._agent = Agent(LoggingUtils())

    def _setup(self, mock_run: Mock, mock_assume: Mock) -> None:
        mock_assume.return_value = AwsSession("AKIA_TEST", "secret", "token")
        mock_run.return_value = dict(_RESULT)

    def test_call_tool(self, mock_run, mock_assume, mock_safe):
        self._setup(mock_run, mock_assume)

        response = self._agent.execute_operation(
            "mcp", "call_tool", _call_tool_operation(), _CREDENTIALS
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual(_RESULT, response.result[ATTRIBUTE_NAME_RESULT])
        mock_safe.assert_called_once_with("aws-mcp.us-east-1.api.aws", 443)
        mock_run.assert_called_once_with(
            _URL,
            ANY,
            "call_tool",
            tool="aws___run_script",
            arguments={"code": _SCRIPT},
            limits=McpLimits(timeout_seconds=15, max_result_bytes=50_000),
            session_id="sess-1",
            protocol_version="2025-06-18",
            keep_session=True,
        )
        auth = mock_run.call_args.args[1]
        self.assertIsInstance(auth.httpx_auth, SigV4HttpxAuth)
        self.assertEqual({"AWS_REGION": "us-east-1"}, auth.meta)

    def test_list_tools(self, mock_run, mock_assume, mock_safe):
        self._setup(mock_run, mock_assume)
        mock_run.return_value = {"tools": [], "truncated": False}

        response = self._agent.execute_operation(
            "mcp",
            "list_tools",
            {
                "trace_id": "t",
                "skip_cache": True,
                "commands": [{"method": "list_tools"}],
            },
            _CREDENTIALS,
        )

        self.assertEqual(
            {"tools": [], "truncated": False}, response.result[ATTRIBUTE_NAME_RESULT]
        )
        mock_run.assert_called_once_with(
            _URL,
            ANY,
            "list_tools",
            limits=McpLimits(),
            session_id=None,
            protocol_version=None,
            keep_session=False,
        )

    def test_errors_carry_their_type(self, mock_run, mock_assume, mock_safe):
        self._setup(mock_run, mock_assume)
        mock_run.side_effect = McpClientError(
            McpErrorCode.AUTH_FAILED, "MCP server rejected the credentials (403)"
        )

        response = self._agent.execute_operation(
            "mcp", "call_tool", _call_tool_operation(), _CREDENTIALS
        )

        self.assertEqual(
            "MCP server rejected the credentials (403)",
            response.result[ATTRIBUTE_NAME_ERROR],
        )
        self.assertEqual("auth_failed", response.result[ATTRIBUTE_NAME_ERROR_TYPE])

    def test_disallowed_host(self, mock_run, mock_assume, mock_safe):
        credentials = {
            "connect_args": {"server": {"url": "https://mcp.example.com/mcp"}}
        }

        response = self._agent.execute_operation(
            "mcp", "call_tool", _call_tool_operation(), credentials
        )

        self.assertEqual(
            "server_not_allowed", response.result[ATTRIBUTE_NAME_ERROR_TYPE]
        )
        mock_run.assert_not_called()
        mock_safe.assert_not_called()

    @patch.dict("os.environ", {"MCD_MCP_ALLOWED_HOSTS": "mcp.example.com"})
    def test_ssrf_block(self, mock_run, mock_assume, mock_safe):
        mock_safe.side_effect = HttpClientError("Destination resolves to 10.0.0.1")
        credentials = {
            "connect_args": {"server": {"url": "https://mcp.example.com/mcp"}}
        }

        response = self._agent.execute_operation(
            "mcp", "call_tool", _call_tool_operation(), credentials
        )

        self.assertEqual(
            "server_not_allowed", response.result[ATTRIBUTE_NAME_ERROR_TYPE]
        )
        mock_safe.assert_called_once_with("mcp.example.com", 443)
        mock_run.assert_not_called()

    def test_only_streamable_http(self, mock_run, mock_assume, mock_safe):
        credentials = {"connect_args": {"server": {"url": _URL, "transport": "sse"}}}

        response = self._agent.execute_operation(
            "mcp", "call_tool", _call_tool_operation(), credentials
        )

        self.assertEqual("bad_request", response.result[ATTRIBUTE_NAME_ERROR_TYPE])
        mock_run.assert_not_called()

    @patch("apollo.agent.agent.StorageProxyClient")
    def test_results_never_use_a_presigned_url(
        self, mock_storage, mock_run, mock_assume, mock_safe
    ):
        self._setup(mock_run, mock_assume)

        response = self._agent.execute_operation(
            "mcp",
            "call_tool",
            _call_tool_operation(response_size_limit_bytes=5),
            _CREDENTIALS,
        )

        self.assertNotIn(ATTRIBUTE_NAME_RESULT_LOCATION, response.result)
        self.assertEqual(_RESULT, response.result[ATTRIBUTE_NAME_RESULT])
        mock_storage.assert_not_called()

    def test_credentials_are_assumed_per_call_and_not_kept(
        self, mock_run, mock_assume, mock_safe
    ):
        self._setup(mock_run, mock_assume)
        client = McpProxyClient(_CREDENTIALS)

        client.call_tool("aws___run_script", {"code": _SCRIPT})
        client.call_tool("aws___run_script", {"code": _SCRIPT})

        self.assertEqual(2, mock_assume.call_count)
        state = json.dumps(vars(client), default=str)
        self.assertNotIn("AKIA_TEST", state)
        self.assertNotIn("token", state)


class TestMcpProxyClient(TestCase):
    @patch.dict(
        "os.environ", {"MCD_MCP_ALLOWED_HOSTS": "idp.example.com,mcp.example.com"}
    )
    @patch("apollo.integrations.mcp.mcp_proxy_client.assert_safe_destination")
    @patch("apollo.integrations.mcp.mcp_proxy_client.run_operation")
    @patch("apollo.integrations.mcp.auth.safe_request")
    def test_oauth_tokens_fetched_per_call_and_not_kept(
        self, mock_token, mock_run, mock_safe
    ):
        token_response = Mock(status_code=200)
        token_response.json.return_value = {"access_token": "tok-secret-1"}
        mock_token.return_value = token_response
        mock_run.return_value = dict(_RESULT)
        client = McpProxyClient(
            {
                "connect_args": {
                    "server": {
                        "url": "https://mcp.example.com/mcp",
                        "auth": {
                            "type": "oauth_client_credentials",
                            "token_url": "https://idp.example.com/token",
                            "client_id": "c",
                        },
                    },
                    "client_secret": "s",
                }
            }
        )

        client.call_tool("t")
        client.call_tool("t")

        self.assertEqual(2, mock_token.call_count)
        self.assertEqual(
            {"Authorization": "Bearer tok-secret-1"},
            mock_run.call_args.args[1].headers,
        )
        self.assertNotIn("tok-secret-1", json.dumps(vars(client), default=str))

    def test_advertised_as_a_native_connection_type(self):
        self.assertIn("mcp", get_native_connection_types())

    def test_log_payload_redacts_tool_arguments(self):
        client = McpProxyClient(_CREDENTIALS)
        for command in (
            {
                "method": "call_tool",
                "kwargs": {"tool": "t", "arguments": {"code": _SCRIPT}},
            },
            {"method": "call_tool", "args": ["t", {"code": _SCRIPT}]},
        ):
            payload = client.log_payload(
                AgentCommands.from_dict({"trace_id": "t", "commands": [command]})
            )
            self.assertNotIn("call_boto3", json.dumps(payload), command)
            self.assertIn("call_tool", json.dumps(payload))

    def test_cache_keeps_servers_apart(self):
        other = json.loads(json.dumps(_CREDENTIALS))
        other["connect_args"]["server"]["url"] = "https://aws-mcp.eu-west-1.api.aws/mcp"
        with patch.dict(ProxyClientFactory._clients_cache, clear=True):
            first = ProxyClientFactory.get_proxy_client(
                "mcp", _CREDENTIALS, False, "AWS"
            )
            again = ProxyClientFactory.get_proxy_client(
                "mcp", _CREDENTIALS, False, "AWS"
            )
            second = ProxyClientFactory.get_proxy_client("mcp", other, False, "AWS")
        self.assertIs(first, again)
        self.assertIsNot(first, second)


# ---- through the Flask route, with the real runner ---------------------------


@pytest.fixture(scope="module")
def flask_client():
    # see tests/credentials/schema/test_endpoint.py: importing main wraps the
    # root logger's formatters, so restore them after this module
    root = logging.getLogger()
    saved = [(h, h.formatter) for h in root.handlers]
    try:
        from apollo.interfaces.generic.main import app  # noqa: PLC0415

        yield app.test_client()
    finally:
        for handler, formatter in saved:
            handler.setFormatter(formatter)


@patch.dict("os.environ", {"MCD_MCP_ALLOWED_HOSTS": "mcp.example.com"})
@patch("apollo.integrations.mcp.mcp_proxy_client.assert_safe_destination")
def test_call_tool_route(mock_safe, flask_client):
    body = {
        "credentials": {
            "connect_args": {
                "server": {
                    "url": "https://mcp.example.com/mcp",
                    "auth": {"type": "none"},
                }
            }
        },
        "operation": {
            "trace_id": "trace-1",
            "skip_cache": True,
            "response_size_limit_bytes": 5,
            "commands": [
                {
                    "method": "call_tool",
                    "kwargs": {
                        "tool": "echo",
                        "arguments": {"code": "result = 1"},
                        "keep_session": True,
                    },
                }
            ],
        },
    }
    with patch(
        "apollo.integrations.mcp.session._streamable_http_streams",
        _memory_streams(_memory_server()),
    ):
        response = flask_client.post("/api/v1/agent/execute/mcp/call_tool", json=body)

    assert response.status_code == 200
    payload = response.get_json()
    assert ATTRIBUTE_NAME_RESULT_LOCATION not in payload
    result = payload[ATTRIBUTE_NAME_RESULT]
    assert result["content"] == [{"type": "text", "text": '{"code": "result = 1"}'}]
    assert result["structured_content"] == {"echo": {"code": "result = 1"}}
    assert result["session_id"] == "memory-session"
    assert result["truncated"] is False
