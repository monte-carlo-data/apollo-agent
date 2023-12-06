import os
import socket
import sys
from telnetlib import Telnet
from typing import Dict, Optional
from unittest import TestCase
from unittest.mock import patch, create_autospec

from apollo.agent.agent import Agent
from apollo.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_TRACE_ID,
    ATTRIBUTE_NAME_RESULT,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.platform import AgentPlatformProvider
from apollo.agent.updater import AgentUpdater
from apollo.agent.utils import AgentUtils
from apollo.validators.validate_network import _DEFAULT_TIMEOUT_SECS
from tests.platform import TestPlatformProvider


class HealthNetworkTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())

    @patch.dict(
        os.environ,
        {
            "PYTHON_VERSION": "3.5",
            "MCD_AGENT_WRAPPER_TYPE": "terraform",
        },
    )
    @patch.object(AgentUtils, "get_outbound_ip_address")
    def test_health_information(self, outboud_mock):
        self._agent.platform_provider = TestPlatformProvider(
            "test platform",
            {
                "container": "test container",
            },
        )
        health_info = self._agent.health_information(trace_id="1234").to_dict()
        self.assertEqual("test platform", health_info["platform"])
        self.assertEqual("local", health_info["version"])
        self.assertEqual("0", health_info["build"])
        self.assertEqual(sys.version, health_info["env"]["sys_version"])
        self.assertEqual("1234", health_info["trace_id"])
        self.assertEqual("3.5", health_info["env"]["PYTHON_VERSION"])
        self.assertEqual("terraform", health_info["env"]["MCD_AGENT_WRAPPER_TYPE"])
        self.assertEqual("test container", health_info["platform_info"]["container"])
        self.assertFalse("MCD_AGENT_IMAGE_TAG" in health_info["env"])
        self.assertFalse("extra" in health_info)

        ip_address = "12.13.14.15"
        outboud_mock.return_value = ip_address
        health_info = self._agent.health_information(
            trace_id="1234", full=True
        ).to_dict()
        self.assertTrue("extra" in health_info)
        self.assertTrue("outbound_ip_address" in health_info["extra"])
        self.assertEqual(ip_address, health_info["extra"]["outbound_ip_address"])

    def test_param_validations(self):
        response = self._agent.validate_telnet_connection(
            None, None, None, trace_id="1234"
        )
        self.assertEqual(
            "host and port are required parameters",
            response.result.get(ATTRIBUTE_NAME_ERROR),
        )
        self.assertEqual("1234", response.result.get(ATTRIBUTE_NAME_TRACE_ID))
        response = self._agent.validate_telnet_connection("localhost", None, None)
        self.assertEqual(
            "host and port are required parameters",
            response.result.get(ATTRIBUTE_NAME_ERROR),
        )
        response = self._agent.validate_telnet_connection("localhost", "text", None)
        self.assertEqual(
            "Invalid value for port parameter: text",
            response.result.get(ATTRIBUTE_NAME_ERROR),
        )
        response = self._agent.validate_telnet_connection("localhost", "123", "text")
        self.assertEqual(
            "Invalid value for timeout parameter: text",
            response.result.get(ATTRIBUTE_NAME_ERROR),
        )

    @patch("socket.socket")
    def test_tcp_open_success(self, mock_socket):
        mock_socket = mock_socket.return_value
        mock_socket.connect_ex.return_value = 0
        response = self._agent.validate_tcp_open_connection(
            "localhost", "123", None, trace_id="1234"
        )
        self.assertEqual("1234", response.result.get(ATTRIBUTE_NAME_TRACE_ID))
        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual(
            "Port 123 is open on localhost",
            response.result.get(ATTRIBUTE_NAME_RESULT).get("message"),
        )

    @patch("socket.socket")
    def test_tcp_open_failure(self, mock_socket):
        mock_socket = mock_socket.return_value
        mock_socket.connect_ex.return_value = 1
        response = self._agent.validate_tcp_open_connection("localhost", "123", None)
        self.assertEqual(
            "Port 123 is closed on localhost.",
            response.result.get(ATTRIBUTE_NAME_ERROR),
        )

    @patch("apollo.validators.validate_network.Telnet")
    def test_telnet_success(self, mock_telnet):
        response = self._agent.validate_telnet_connection("localhost", "123", None)
        print(response)
        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual(
            "Telnet connection for localhost:123 is usable.",
            response.result.get(ATTRIBUTE_NAME_RESULT).get("message"),
        )

    @patch("apollo.validators.validate_network.Telnet")
    def test_telnet_timeout(self, mock_telnet):
        mock_telnet.side_effect = socket.timeout

        response = self._agent.validate_telnet_connection("localhost", "123", "11")
        mock_telnet.assert_called_with("localhost", 123, 11)
        self.assertEqual(
            "Socket timeout for localhost:123. Connection unusable.",
            response.result.get(ATTRIBUTE_NAME_ERROR),
        )

    @patch("apollo.validators.validate_network.Telnet")
    def test_telnet_read_failed(self, mock_telnet):
        mock_session = create_autospec(Telnet)
        mock_telnet.return_value.__enter__.return_value = mock_session
        mock_session.read_very_eager.side_effect = EOFError

        response = self._agent.validate_telnet_connection("localhost", "123", None)
        mock_telnet.assert_called_with("localhost", 123, _DEFAULT_TIMEOUT_SECS)
        self.assertEqual(
            "Telnet connection for localhost:123 is unusable.",
            response.result.get(ATTRIBUTE_NAME_ERROR),
        )
