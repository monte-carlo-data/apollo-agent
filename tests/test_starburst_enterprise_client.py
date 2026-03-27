import base64
import json
import logging
import os
from unittest import TestCase
from unittest.mock import (
    Mock,
    patch,
    MagicMock,
)

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.integrations.ctp.registry import CtpRegistry
from apollo.interfaces.lambda_function.json_log_formatter import JsonLogFormatter

_STARBURST_CREDENTIALS = {
    "host": "example.starburst.io",
    "port": "443",
    "http_scheme": "https",
    "catalog": "fizz",
    "schema": "buzz",
    "user": "foo",
    "password": "bar",
}

_EXPECTED_AUTH_HEADER = f"Basic {base64.b64encode(b'foo:bar').decode('ascii')}"


class StarburstEnterpriseHttpTests(TestCase):
    """Tests for Starburst Enterprise HTTP request functionality."""

    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor

    @patch("requests.request")
    @patch("trino.dbapi.connect")
    def test_do_http_request(self, mock_connect, mock_request):
        """Test that do_http_request makes proper HTTP calls using credentials."""
        mock_connect.return_value = self._mock_connection

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"result": "success"}'
        mock_response.json.return_value = {"result": "success"}
        mock_request.return_value = mock_response

        operation_dict = {
            "trace_id": "1234",
            "skip_cache": True,
            "commands": [
                {
                    "method": "do_http_request",
                    "kwargs": {
                        "path": "/api/v1/test",
                        "http_method": "GET",
                    },
                },
            ],
        }

        response = self._agent.execute_operation(
            "starburst-enterprise",
            "http_request",
            operation_dict,
            {"connect_args": _STARBURST_CREDENTIALS},
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        result = response.result.get(ATTRIBUTE_NAME_RESULT)
        self.assertEqual({"result": "success"}, result)

        # Verify the HTTP request was made with correct URL and auth
        mock_request.assert_called_once()
        call_args = mock_request.call_args
        self.assertEqual(call_args[0][0], "GET")
        self.assertEqual(
            call_args[0][1], "https://example.starburst.io:443/api/v1/test"
        )
        self.assertIn("Authorization", call_args[1]["headers"])
        self.assertEqual(
            call_args[1]["headers"]["Authorization"], _EXPECTED_AUTH_HEADER
        )

    @patch("requests.request")
    @patch("trino.dbapi.connect")
    def test_do_http_request_with_params(self, mock_connect, mock_request):
        """Test that do_http_request passes query params correctly."""
        mock_connect.return_value = self._mock_connection

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"data": []}'
        mock_response.json.return_value = {"data": []}
        mock_request.return_value = mock_response

        operation_dict = {
            "trace_id": "1234",
            "skip_cache": True,
            "commands": [
                {
                    "method": "do_http_request",
                    "kwargs": {
                        "path": "/api/v1/search",
                        "http_method": "GET",
                        "params": {"domain": "test_domain", "limit": 100},
                    },
                },
            ],
        }

        response = self._agent.execute_operation(
            "starburst-enterprise",
            "http_request",
            operation_dict,
            {"connect_args": _STARBURST_CREDENTIALS},
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))

        # Verify params were passed
        call_args = mock_request.call_args
        self.assertEqual(
            call_args[1].get("params"), {"domain": "test_domain", "limit": 100}
        )

    @patch("requests.request")
    @patch("trino.dbapi.connect")
    def test_do_http_request_post_with_payload(self, mock_connect, mock_request):
        """Test that do_http_request handles POST with JSON payload."""
        mock_connect.return_value = self._mock_connection

        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.text = '{"id": "new-123"}'
        mock_response.json.return_value = {"id": "new-123"}
        mock_request.return_value = mock_response

        operation_dict = {
            "trace_id": "1234",
            "skip_cache": True,
            "commands": [
                {
                    "method": "do_http_request",
                    "kwargs": {
                        "path": "/api/v1/resources",
                        "http_method": "POST",
                        "payload": {"name": "New Resource", "type": "test"},
                    },
                },
            ],
        }

        response = self._agent.execute_operation(
            "starburst-enterprise",
            "http_request",
            operation_dict,
            {"connect_args": _STARBURST_CREDENTIALS},
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        result = response.result.get(ATTRIBUTE_NAME_RESULT)
        self.assertEqual({"id": "new-123"}, result)

        # Verify POST method and JSON payload
        call_args = mock_request.call_args
        self.assertEqual(call_args[0][0], "POST")
        self.assertEqual(
            call_args[1].get("json"), {"name": "New Resource", "type": "test"}
        )

    @patch("requests.request")
    @patch("trino.dbapi.connect")
    def test_http_request_error_handling(self, mock_connect, mock_request):
        """Test that HTTP errors are properly handled."""
        from requests import HTTPError

        mock_connect.return_value = self._mock_connection

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        mock_response.reason = "Not Found"
        mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)
        mock_request.return_value = mock_response

        operation_dict = {
            "trace_id": "1234",
            "skip_cache": True,
            "commands": [
                {
                    "method": "do_http_request",
                    "kwargs": {"path": "/api/v1/nonexistent"},
                },
            ],
        }

        response = self._agent.execute_operation(
            "starburst-enterprise",
            "http_request",
            operation_dict,
            {"connect_args": _STARBURST_CREDENTIALS},
        )

        self.assertIn("Not Found", response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual("HTTPError", response.result.get(ATTRIBUTE_NAME_ERROR_TYPE))


class StarburstEnterpriseCredentialShapeTests(TestCase):
    """Verify the proxy client init accepts both DC-style and CTP-resolved credentials.

    DC path (today): DC plugin builds connect_args including ssl_options (unresolved)
    and sends them to the agent. The proxy client pops ssl_options and handles SSL itself.

    CTP path (after Phase 2): flat credentials go through CTP, which resolves ssl_options
    into a verify value before the proxy client is created. The proxy client receives
    clean connect_args with no ssl_options.

    In both paths trino.dbapi.connect must receive the same effective arguments.
    """

    _HOST = "example.starburst.io"
    _PORT_INT = 8443
    _PORT_STR = "8443"
    _USER = "admin"
    _PASSWORD = "secret"
    _CA_PEM = "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----"

    def setUp(self) -> None:
        pass  # connector registered by _discover() via _ensure_initialized()

    def _dc_creds(self, **ssl_kwargs):
        """Build DC-style credentials: connect_args with ssl_options not yet resolved."""
        return {
            "connect_args": {
                "host": self._HOST,
                "port": self._PORT_INT,
                "user": self._USER,
                "password": self._PASSWORD,
                "http_scheme": "https",
                **ssl_kwargs,
            }
        }

    def _ctp_creds(self, **flat_kwargs):
        """Build CTP-resolved credentials from flat input via the registry."""
        return CtpRegistry.resolve(
            "starburst-enterprise",
            {
                "host": self._HOST,
                "port": self._PORT_STR,
                "user": self._USER,
                "password": self._PASSWORD,
                **flat_kwargs,
            },
        )

    # ------------------------------------------------------------------
    # No SSL
    # ------------------------------------------------------------------

    @patch("trino.dbapi.connect")
    def test_dc_no_ssl(self, mock_connect):
        """DC sends empty ssl_options — no verify passed to trino."""
        from apollo.integrations.db.starburst_enterprise_proxy_client import (
            StarburstEnterpriseProxyClient,
        )

        mock_connect.return_value = Mock()
        StarburstEnterpriseProxyClient(
            credentials=self._dc_creds(ssl_options={}), platform="test"
        )
        self.assertNotIn("verify", mock_connect.call_args.kwargs)
        self.assertNotIn("ssl_options", mock_connect.call_args.kwargs)

    @patch("trino.dbapi.connect")
    def test_ctp_no_ssl(self, mock_connect):
        """CTP with no ssl_options — no verify passed to trino."""
        from apollo.integrations.db.starburst_enterprise_proxy_client import (
            StarburstEnterpriseProxyClient,
        )

        mock_connect.return_value = Mock()
        StarburstEnterpriseProxyClient(credentials=self._ctp_creds(), platform="test")
        self.assertNotIn("verify", mock_connect.call_args.kwargs)
        self.assertNotIn("ssl_options", mock_connect.call_args.kwargs)

    # ------------------------------------------------------------------
    # CA data — cert written to file, verify=<path>
    # ------------------------------------------------------------------

    @patch("trino.dbapi.connect")
    def test_dc_ca_data(self, mock_connect):
        """DC sends ssl_options with ca_data — proxy client writes cert, verify=<path>."""
        from apollo.integrations.db.starburst_enterprise_proxy_client import (
            StarburstEnterpriseProxyClient,
        )

        mock_connect.return_value = Mock()
        StarburstEnterpriseProxyClient(
            credentials=self._dc_creds(ssl_options={"ca_data": self._CA_PEM}),
            platform="test",
        )
        verify = mock_connect.call_args.kwargs.get("verify")
        self.assertIsInstance(verify, str)
        self.assertTrue(os.path.exists(verify))
        os.unlink(verify)

    @patch("trino.dbapi.connect")
    def test_ctp_ca_data(self, mock_connect):
        """CTP resolves ssl_options ca_data to verify=<path> before proxy client is created."""
        from apollo.integrations.db.starburst_enterprise_proxy_client import (
            StarburstEnterpriseProxyClient,
        )

        mock_connect.return_value = Mock()
        StarburstEnterpriseProxyClient(
            credentials=self._ctp_creds(ssl_options={"ca_data": self._CA_PEM}),
            platform="test",
        )
        verify = mock_connect.call_args.kwargs.get("verify")
        self.assertIsInstance(verify, str)
        self.assertTrue(os.path.exists(verify))
        os.unlink(verify)

    # ------------------------------------------------------------------
    # SSL disabled — verify=False
    # ------------------------------------------------------------------

    @patch("trino.dbapi.connect")
    def test_dc_ssl_disabled(self, mock_connect):
        """DC sends verify=False + ssl_options disabled — trino gets verify=False."""
        from apollo.integrations.db.starburst_enterprise_proxy_client import (
            StarburstEnterpriseProxyClient,
        )

        mock_connect.return_value = Mock()
        # DC sets verify=False in connection_args when disabled, and includes ssl_options
        StarburstEnterpriseProxyClient(
            credentials=self._dc_creds(verify=False, ssl_options={"disabled": True}),
            platform="test",
        )
        self.assertIs(False, mock_connect.call_args.kwargs.get("verify"))

    @patch("trino.dbapi.connect")
    def test_ctp_ssl_disabled(self, mock_connect):
        """CTP resolves ssl_options disabled to verify=False before proxy client is created."""
        from apollo.integrations.db.starburst_enterprise_proxy_client import (
            StarburstEnterpriseProxyClient,
        )

        mock_connect.return_value = Mock()
        StarburstEnterpriseProxyClient(
            credentials=self._ctp_creds(ssl_options={"disabled": True}),
            platform="test",
        )
        self.assertIs(False, mock_connect.call_args.kwargs.get("verify"))


class _ListHandler(logging.Handler):
    def __init__(self, records):
        super().__init__()
        self._records = records

    def emit(self, record):
        self._records.append(record)


class StarburstEnterpriseCtpCredentialSafetyTests(TestCase):
    _HOST = "cluster.example.starburst.io"
    _USER = "svc_account@example.com"
    _PASSWORD = "s3cr3t_p@ssw0rd!"

    _OPERATION = {
        "trace_id": "ctp-safety-test",
        "skip_cache": True,
        "commands": [
            {"method": "execute", "args": ["SELECT 1"]},
        ],
    }

    def setUp(self):
        self._agent = Agent(LoggingUtils())
        # connector registered by _discover() via _ensure_initialized()
        self._log_records = []
        self._log_handler = _ListHandler(self._log_records)
        logging.getLogger().addHandler(self._log_handler)

    def tearDown(self):
        logging.getLogger().removeHandler(self._log_handler)

    def _assert_no_credential_leak(self, response) -> None:
        serialized = json.dumps(response.result, default=str)
        self.assertNotIn(self._PASSWORD, serialized, "password leaked in response")
        self.assertNotIn(self._USER, serialized, "username leaked in response")

    @patch("trino.dbapi.connect")
    def test_missing_required_host_is_actionable_and_safe(self, mock_connect):
        """CTP validation: missing host produces an actionable error without leaking creds."""
        response = self._agent.execute_operation(
            "starburst-enterprise",
            "query",
            self._OPERATION,
            # host intentionally omitted — Required[str] in schema
            {"port": "8443", "user": self._USER, "password": self._PASSWORD},
        )
        mock_connect.assert_not_called()
        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn("host", error)
        self._assert_no_credential_leak(response)

    @patch("trino.dbapi.connect")
    def test_connect_failure_is_actionable_and_safe(self, mock_connect):
        """Connection failure exposes the hostname but not the password."""
        mock_connect.side_effect = Exception(
            f"Failed to connect to Trino at {self._HOST}:8443"
        )
        response = self._agent.execute_operation(
            "starburst-enterprise",
            "query",
            self._OPERATION,
            {
                "host": self._HOST,
                "port": "8443",
                "user": self._USER,
                "password": self._PASSWORD,
            },
        )
        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn(self._HOST, error)
        self._assert_no_credential_leak(response)

    @patch("trino.dbapi.connect")
    def test_auth_failure_is_actionable_and_safe(self, mock_connect):
        """Auth failure from Trino surfaces a useful error without leaking credentials."""
        mock_connect.side_effect = Exception(
            "401 Unauthorized: invalid username or password"
        )
        response = self._agent.execute_operation(
            "starburst-enterprise",
            "query",
            self._OPERATION,
            {
                "host": self._HOST,
                "port": "8443",
                "user": self._USER,
                "password": self._PASSWORD,
            },
        )
        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn("401", error)
        self._assert_no_credential_leak(response)

    @patch("trino.dbapi.connect")
    def test_log_output_does_not_leak_credentials(self, mock_connect):
        """JsonLogFormatter (Datadog/Lambda path) never emits the password."""
        mock_connect.side_effect = Exception(f"Failed to connect to {self._HOST}")
        self._agent.execute_operation(
            "starburst-enterprise",
            "query",
            self._OPERATION,
            {
                "host": self._HOST,
                "port": "8443",
                "user": self._USER,
                "password": self._PASSWORD,
            },
        )
        formatter = JsonLogFormatter()
        for record in self._log_records:
            output = formatter.format(record)
            self.assertNotIn(self._PASSWORD, output)
