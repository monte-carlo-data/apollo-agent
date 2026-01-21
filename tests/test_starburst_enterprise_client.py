import base64
from unittest import TestCase
from unittest.mock import (
    Mock,
    patch,
    MagicMock,
)

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.agent.logging_utils import LoggingUtils

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
