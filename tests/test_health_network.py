import os
import socket
import sys
from telnetlib import Telnet
from typing import Any
from unittest import TestCase
from unittest.mock import patch, Mock, create_autospec

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.validators.validate_network import ValidateNetwork, _DEFAULT_TIMEOUT_SECS


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
    def test_health_information(self):
        self._agent.platform = "test platform"
        self._agent.platform_info = {
            "container": "test container",
        }
        health_info = self._agent.health_information().to_dict()
        self.assertEqual("test platform", health_info["platform"])
        self.assertEqual("local", health_info["version"])
        self.assertEqual("0", health_info["build"])
        self.assertEqual(sys.version, health_info["env"]["sys_version"])
        self.assertEqual("3.5", health_info["env"]["PYTHON_VERSION"])
        self.assertEqual("terraform", health_info["env"]["MCD_AGENT_WRAPPER_TYPE"])
        self.assertEqual("test container", health_info["platform_info"]["container"])
        self.assertFalse("MCD_AGENT_IMAGE_TAG" in health_info["env"])

    def test_param_validations(self):
        response = ValidateNetwork.validate_telnet_connection(None, None, None)
        self.assertEqual(
            "host and port are required parameters", response.result.get("__error__")
        )
        response = ValidateNetwork.validate_telnet_connection("localhost", None, None)
        self.assertEqual(
            "host and port are required parameters", response.result.get("__error__")
        )
        response = ValidateNetwork.validate_telnet_connection("localhost", "text", None)
        self.assertEqual(
            "Invalid value for port parameter: text", response.result.get("__error__")
        )
        response = ValidateNetwork.validate_telnet_connection(
            "localhost", "123", "text"
        )
        self.assertEqual(
            "Invalid value for timeout parameter: text",
            response.result.get("__error__"),
        )

    @patch("socket.socket")
    def test_tcp_open_success(self, mock_socket):
        mock_socket = mock_socket.return_value
        mock_socket.connect_ex.return_value = 0
        response = ValidateNetwork.validate_tcp_open_connection(
            "localhost", "123", None
        )
        self.assertIsNone(response.result.get("__error__"))
        self.assertEqual(
            "Port 123 is open on localhost",
            response.result.get("message"),
        )

    @patch("socket.socket")
    def test_tcp_open_failure(self, mock_socket):
        mock_socket = mock_socket.return_value
        mock_socket.connect_ex.return_value = 1
        response = ValidateNetwork.validate_tcp_open_connection(
            "localhost", "123", None
        )
        self.assertEqual(
            "Port 123 is closed on localhost.", response.result.get("__error__")
        )

    @patch("apollo.validators.validate_network.Telnet")
    def test_telnet_success(self, mock_telnet):
        response = ValidateNetwork.validate_telnet_connection("localhost", "123", None)
        print(response)
        self.assertIsNone(response.result.get("__error__"))
        self.assertEqual(
            "Telnet connection for localhost:123 is usable.",
            response.result.get("message"),
        )

    @patch("apollo.validators.validate_network.Telnet")
    def test_telnet_timeout(self, mock_telnet):
        mock_telnet.side_effect = socket.timeout

        response = ValidateNetwork.validate_telnet_connection("localhost", "123", "11")
        mock_telnet.assert_called_with("localhost", 123, 11)
        self.assertEqual(
            "Socket timeout for localhost:123. Connection unusable.",
            response.result.get("__error__"),
        )

    @patch("apollo.validators.validate_network.Telnet")
    def test_telnet_read_failed(self, mock_telnet):
        mock_session = create_autospec(Telnet)
        mock_telnet.return_value.__enter__.return_value = mock_session
        mock_session.read_very_eager.side_effect = EOFError

        response = ValidateNetwork.validate_telnet_connection("localhost", "123", None)
        mock_telnet.assert_called_with("localhost", 123, _DEFAULT_TIMEOUT_SECS)
        self.assertEqual(
            "Telnet connection for localhost:123 is unusable.",
            response.result.get("__error__"),
        )
