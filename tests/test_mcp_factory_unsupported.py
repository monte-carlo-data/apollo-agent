"""
MCP behaviour that must hold on every image, including those without the MCP
SDK (generic, cloudrun, azure): no `mcp` imports at module level here.
"""

import sys
from unittest import TestCase
from unittest.mock import patch

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.proxy_client_factory import get_native_connection_types
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_ERROR_TYPE,
)

_CREDENTIALS = {
    "connect_args": {"server": {"url": "https://aws-mcp.us-east-1.api.aws/mcp"}}
}


class TestMcpWithoutSdk(TestCase):
    @patch("apollo.agent.proxy_client_factory.find_spec")
    def test_advertised_only_when_the_sdk_is_installed(self, mock_find_spec):
        mock_find_spec.return_value = None
        self.assertNotIn("mcp", get_native_connection_types())
        mock_find_spec.assert_called_with("mcp")

        mock_find_spec.return_value = object()
        types = get_native_connection_types()
        self.assertIn("mcp", types)
        self.assertEqual(sorted(types), types)

    @patch("apollo.integrations.mcp.mcp_proxy_client.assert_safe_destination")
    def test_calls_fail_cleanly_without_the_sdk(self, _):
        # a None entry makes `import mcp` (and our session module) raise ImportError
        with patch.dict(
            sys.modules,
            {
                "mcp": None,
                "apollo.integrations.mcp.auth": None,
                "apollo.integrations.mcp.session": None,
            },
        ):
            response = Agent(LoggingUtils()).execute_operation(
                "mcp",
                "call_tool",
                {
                    "trace_id": "t",
                    "skip_cache": True,
                    "commands": [{"method": "call_tool", "kwargs": {"tool": "x"}}],
                },
                _CREDENTIALS,
            )

        self.assertEqual("mcp_unsupported", response.result[ATTRIBUTE_NAME_ERROR_TYPE])
        self.assertIn("not supported", response.result[ATTRIBUTE_NAME_ERROR])
