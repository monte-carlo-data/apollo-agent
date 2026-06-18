import os
import socket
import sys
from unittest import TestCase
from unittest.mock import patch, create_autospec, Mock

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_TRACE_ID,
    ATTRIBUTE_NAME_RESULT,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.utils import AgentUtils
from tests.platform_provider import TestPlatformProvider


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
        self.assertEqual(sys.version, health_info["env"]["PYTHON_SYS_VERSION"])
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

    # Tests use a public IP literal so assert_safe_destination short-circuits
    # without hitting DNS. `localhost` is now rejected by the SSRF guard —
    # covered by dedicated regression tests further down.

    @patch("socket.socket")
    def test_tcp_open_success(self, mock_socket):
        mock_socket = mock_socket.return_value
        mock_socket.connect_ex.return_value = 0
        response = self._agent.validate_tcp_open_connection(
            "93.184.216.34", "123", None, trace_id="1234"
        )
        self.assertEqual("1234", response.result.get(ATTRIBUTE_NAME_TRACE_ID))
        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual(
            "Port 123 is open on 93.184.216.34",
            response.result.get(ATTRIBUTE_NAME_RESULT).get("message"),
        )

    @patch("socket.socket")
    def test_tcp_open_failure(self, mock_socket):
        mock_socket = mock_socket.return_value
        mock_socket.connect_ex.return_value = 1
        response = self._agent.validate_tcp_open_connection(
            "93.184.216.34", "123", None
        )
        self.assertEqual(
            "Port 123 is closed on 93.184.216.34.",
            response.result.get(ATTRIBUTE_NAME_ERROR),
        )

    # `telnetlib` was removed from the Python stdlib in 3.13 (PEP 594). The
    # retired telnet check is kept for frontend compatibility but now maps to
    # the TCP-open validation, so its responses match TCP-open's.

    @patch("socket.socket")
    def test_telnet_maps_to_tcp_open_success(self, mock_socket):
        mock_socket = mock_socket.return_value
        mock_socket.connect_ex.return_value = 0
        response = self._agent.validate_telnet_connection("93.184.216.34", "123", None)
        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual(
            "Port 123 is open on 93.184.216.34",
            response.result.get(ATTRIBUTE_NAME_RESULT).get("message"),
        )

    @patch("socket.socket")
    def test_telnet_maps_to_tcp_open_closed(self, mock_socket):
        mock_socket = mock_socket.return_value
        mock_socket.connect_ex.return_value = 1
        response = self._agent.validate_telnet_connection("93.184.216.34", "123", None)
        self.assertEqual(
            "Port 123 is closed on 93.184.216.34.",
            response.result.get(ATTRIBUTE_NAME_ERROR),
        )

    # --- TOCTOU regression tests ------------------------------------------

    @patch("apollo.validators.validate_network.socket.socket")
    @patch("apollo.integrations.http.url_safety.socket.getaddrinfo")
    def test_tcp_open_resolves_once_no_toctou(self, mock_gai, mock_socket_cls):
        """Regression: TCP validator must resolve DNS once (via assert_safe_destination)
        and connect by IP, not re-resolve. Mock getaddrinfo to return a public IP;
        assert connect_ex was called with that IP, not with the hostname."""
        mock_gai.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 80)),
        ]
        mock_sock = mock_socket_cls.return_value
        mock_sock.connect_ex.return_value = 0

        response = self._agent.validate_tcp_open_connection(
            "example.com", "80", None, trace_id=None
        )

        # getaddrinfo called once (by assert_safe_destination); connect_ex called
        # with the resolved IP literal, not the hostname.
        self.assertEqual(mock_gai.call_count, 1)
        mock_sock.connect_ex.assert_called_once_with(("93.184.216.34", 80))
        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))

    @patch("apollo.validators.validate_network.socket.socket")
    @patch("apollo.integrations.http.url_safety.socket.getaddrinfo")
    def test_telnet_resolves_once_no_toctou(self, mock_gai, mock_socket_cls):
        """Regression: the telnet endpoint now maps to TCP-open, which must
        resolve DNS once (via assert_safe_destination) and connect by IP, not
        re-resolve. Assert connect_ex was called with the resolved IP."""
        mock_gai.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 23)),
        ]
        mock_sock = mock_socket_cls.return_value
        mock_sock.connect_ex.return_value = 0

        response = self._agent.validate_telnet_connection(
            "example.com", "23", None, trace_id=None
        )

        self.assertEqual(mock_gai.call_count, 1)
        mock_sock.connect_ex.assert_called_once_with(("93.184.216.34", 23))
        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))

    # --- SSRF guard regression tests --------------------------------------

    def test_tcp_open_rejects_metadata_ip(self):
        """The SSRF guard refuses to probe cloud metadata services even via
        the troubleshooting endpoint."""
        response = self._agent.validate_tcp_open_connection(
            "169.254.169.254", "80", None
        )
        self.assertIn(
            "blocked address",
            response.result.get(ATTRIBUTE_NAME_ERROR) or "",
        )

    def test_tcp_open_rejects_localhost(self):
        response = self._agent.validate_tcp_open_connection("localhost", "80", None)
        self.assertIn(
            "localhost",
            response.result.get(ATTRIBUTE_NAME_ERROR) or "",
        )

    def test_telnet_rejects_metadata_ip(self):
        response = self._agent.validate_telnet_connection("169.254.169.254", "80", None)
        self.assertIn(
            "blocked address",
            response.result.get(ATTRIBUTE_NAME_ERROR) or "",
        )

    def test_telnet_rejects_localhost(self):
        """F4: SSRF guard symmetry — Telnet mirrors TCP's localhost rejection."""
        response = self._agent.validate_telnet_connection("localhost", "80", None)
        self.assertIn(
            "localhost",
            response.result.get(ATTRIBUTE_NAME_ERROR) or "",
        )

    def test_http_rejects_metadata_ip(self):
        """The HTTP troubleshooting endpoint must not be a way to fetch
        IMDS credentials. The SSRF guard fires at the TCP layer and the
        error is surfaced as a ConnectionFailedError."""
        response = self._agent.validate_http_connection(
            "http://169.254.169.254/latest/meta-data/", "true", None, trace_id=None
        )
        self.assertIn(
            "blocked address",
            response.result.get(ATTRIBUTE_NAME_ERROR) or "",
        )

    @patch("apollo.validators.validate_network.socket.getaddrinfo")
    def test_dns_lookup(self, getaddrinfo_mock):
        getaddrinfo_mock.return_value = [
            (0, 0, 0, "", ("1.2.3.4", 0)),
            (0, 0, 0, "", ("1.2.3.4", 0)),
            (0, 0, 0, "", ("5.6.7.8", 0)),
        ]
        response = self._agent.perform_dns_lookup("localhost", None, None)
        self.assertEqual(
            "Host localhost resolves to: 1.2.3.4, 5.6.7.8",
            response.result.get(ATTRIBUTE_NAME_RESULT).get("message"),
        )

    @patch("apollo.validators.validate_network.safe_request")
    def test_http_connection(self, safe_request_mock):
        response_mock = Mock()
        response_mock.status_code = 200
        response_mock.reason = "OK"
        response_mock.content = b"foo"
        safe_request_mock.return_value = response_mock
        response = self._agent.validate_http_connection(
            "https://foo.bar", "true", None, trace_id=None
        )
        self.assertEqual(
            "URL https://foo.bar responded with status: 200 (OK) and content: foo",
            response.result.get(ATTRIBUTE_NAME_RESULT).get("message"),
        )

        response = self._agent.validate_http_connection(
            "https://foo.bar", None, None, trace_id=None
        )
        self.assertEqual(
            "URL https://foo.bar responded with status: 200 (OK)",
            response.result.get(ATTRIBUTE_NAME_RESULT).get("message"),
        )


class TestHealthEasyAuthIntegration(TestCase):
    """Tests for Easy Auth enforcement verification in the health endpoint.

    Imports of ``apollo.interfaces.generic.main`` are deferred to setUp so
    that the module-level ``RedactFormatterWrapper`` monkey-patch on the root
    logger does not corrupt pytest's log-capture handler for the rest of the
    test session (it mutates ``record.msg`` before %-formatting, which breaks
    any ``logger.warning("… %s", val)`` call elsewhere).
    """

    def setUp(self) -> None:
        # Importing generic/main.py wraps every root-logger handler with
        # RedactFormatterWrapper (module-level side effect).  The wrapper
        # mutates record.msg *before* %-formatting, which breaks any
        # logger.warning("… %s", val) call in later tests.  Snapshot
        # formatters *before* the import so we can restore them in tearDown.
        import logging

        root = logging.getLogger()
        self._original_formatters = [(h, h.formatter) for h in root.handlers]

        # Deferred imports — see class docstring.
        from apollo.interfaces.azure.auth import (
            _EASY_AUTH_PROBE_HEADER,
            _EASY_AUTH_PROBE_TOKEN,
        )
        from apollo.interfaces.generic import main as generic_main
        from apollo.interfaces.generic.main import app

        self._generic_main = generic_main
        self._probe_header = _EASY_AUTH_PROBE_HEADER
        self._probe_token = _EASY_AUTH_PROBE_TOKEN
        self.client = app.test_client()
        self._prev_provider = generic_main.agent.platform_provider

    def tearDown(self) -> None:
        self._generic_main.agent.platform_provider = self._prev_provider
        # Restore original formatters to undo RedactFormatterWrapper.
        for handler, fmt in self._original_formatters:
            handler.setFormatter(fmt)

    def _set_azure_sp_provider(self) -> None:
        """Set an AzurePlatformProvider with SP auth enabled."""
        from apollo.interfaces.azure.azure_platform import AzurePlatformProvider

        with patch.dict(
            os.environ,
            {"MCD_AUTH_TYPE": "AZURE_FUNCTION_SERVICE_PRINCIPAL"},
        ):
            self._generic_main.agent.platform_provider = AzurePlatformProvider()

    def test_health_probe_request_returns_200_shortcircuit(self):
        """Platform provider short-circuits probe requests with a minimal 200."""
        self._set_azure_sp_provider()
        resp = self.client.get(
            "/api/v1/test/health",
            headers={self._probe_header: self._probe_token},
        )
        assert resp.status_code == 200
        assert resp.get_json() == {"status": "up"}

    def test_health_returns_200_when_easy_auth_verified(self):
        """SP provider with Easy Auth verified -> normal 200 health response."""
        self._set_azure_sp_provider()
        with patch(
            "apollo.interfaces.azure.auth.verify_easy_auth_enforcement",
            return_value=None,
        ):
            resp = self.client.get("/api/v1/test/health")
            assert resp.status_code == 200
            data = resp.get_json()
            assert "easy_auth_error" not in data

    def test_health_returns_503_when_easy_auth_not_verified(self):
        """SP provider with Easy Auth NOT verified -> 503 with generic error."""
        self._set_azure_sp_provider()
        with patch(
            "apollo.interfaces.azure.auth.verify_easy_auth_enforcement",
            return_value="Easy Auth is NOT intercepting unauthenticated requests",
        ):
            resp = self.client.get("/api/v1/test/health")
            assert resp.status_code == 503
            data = resp.get_json()
            assert data["easy_auth_error"] == (
                "Easy Auth enforcement could not be verified"
            )

    def test_health_returns_200_without_platform_provider(self):
        """Without a platform provider, no Easy Auth check — normal 200."""
        self._generic_main.agent.platform_provider = None
        resp = self.client.get("/api/v1/test/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "easy_auth_error" not in data
