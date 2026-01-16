from copy import deepcopy
from unittest import TestCase
from unittest.mock import create_autospec, patch, call, mock_open

from requests import Response, HTTPError

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_ERROR_TYPE,
    ATTRIBUTE_NAME_ERROR_ATTRS,
    ATTRIBUTE_VALUE_REDACTED,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.http.http_proxy_client import HttpProxyClient

_HTTP_USER_AGENT = "TestUserAgent"

_HTTP_OPERATION = {
    "trace_id": "1234",
    "commands": [
        {
            "method": "do_request",
            "kwargs": {
                "url": "https://test.com/path",
                "http_method": "GET",
                "payload": {},
                "user_agent": _HTTP_USER_AGENT,
                "additional_headers": None,
                "retry_status_code_ranges": None,
                "verify_ssl": None,
            },
        }
    ],
}
_HTTP_CREDENTIALS = {"token": "123_token"}


class TestHttpClient(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())

    @patch("requests.request")
    def test_http_request(self, mock_request):
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        response = self._agent.execute_operation(
            "http",
            "do_request",
            _HTTP_OPERATION,
            _HTTP_CREDENTIALS,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": f"Bearer {_HTTP_CREDENTIALS['token']}",
                "User-Agent": _HTTP_USER_AGENT,
            },
        )
        mock_response.assert_has_calls(
            [
                call.raise_for_status(),
                call.json(),
            ]
        )
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        self.assertEqual(expected_result, response.result.get(ATTRIBUTE_NAME_RESULT))

    @patch("requests.request")
    @patch("apollo.agent.agent.logger.info")
    def test_http_request_data_redacted(self, mock_info, mock_request):
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        operation = deepcopy(_HTTP_OPERATION)
        operation["commands"][0]["kwargs"]["data"] = "client_secret=1234&client_id=4321"
        operation["commands"][0]["kwargs"]["additional_headers"] = {
            "Authorization": f"Bearer {_HTTP_CREDENTIALS['token']}",
            "client_secret": "1234",
            "client_id": "4321",
            "auth_key": "1234",
        }
        self._agent.execute_operation(
            "http",
            "do_request",
            operation,
            _HTTP_CREDENTIALS,
        )
        mock_info.assert_called_with(
            f"Executing operation: http/do_request",
            extra={
                "mcd_operation_name": "do_request",
                "response_size_limit_bytes": 0,
                "compress_response_threshold_bytes": 0,
                "response_type": "json",
                "skip_cache": False,
                "compress_response_file": False,
                "mcd_trace_id": "1234",
                "commands": [
                    {
                        "method": "do_request",
                        "kwargs": {
                            "url": "https://test.com/path",
                            "http_method": "GET",
                            "payload": ATTRIBUTE_VALUE_REDACTED,
                            "user_agent": ATTRIBUTE_VALUE_REDACTED,
                            "additional_headers": {
                                "Authorization": ATTRIBUTE_VALUE_REDACTED,
                                "client_secret": ATTRIBUTE_VALUE_REDACTED,
                                "client_id": ATTRIBUTE_VALUE_REDACTED,
                                "auth_key": ATTRIBUTE_VALUE_REDACTED,
                            },
                            "retry_status_code_ranges": None,
                            "verify_ssl": None,
                            "data": ATTRIBUTE_VALUE_REDACTED,
                        },
                    }
                ],
            },
        )

    @patch("requests.request")
    def test_http_request_with_custom_auth_type(self, mock_request):
        credentials = {
            "auth_type": "Token",
            "token": "1234",
        }
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        response = self._agent.execute_operation(
            "http",
            "do_request",
            _HTTP_OPERATION,
            credentials,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": f"{credentials['auth_type']} {credentials['token']}",
                "User-Agent": _HTTP_USER_AGENT,
            },
        )
        mock_response.assert_has_calls(
            [
                call.raise_for_status(),
                call.json(),
            ]
        )
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        self.assertEqual(expected_result, response.result.get(ATTRIBUTE_NAME_RESULT))

    @patch("requests.request")
    def test_http_request_with_custom_auth_header(self, mock_request):
        credentials = {
            "auth_header": "Api-Key",
            "auth_type": None,
            "token": "1234",
        }
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        response = self._agent.execute_operation(
            "http",
            "do_request",
            _HTTP_OPERATION,
            credentials,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                credentials["auth_header"]: credentials["token"],
                "User-Agent": _HTTP_USER_AGENT,
            },
        )
        mock_response.assert_has_calls(
            [
                call.raise_for_status(),
                call.json(),
            ]
        )
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        self.assertEqual(expected_result, response.result.get(ATTRIBUTE_NAME_RESULT))

    @patch("requests.request")
    def test_http_request_with_no_auth(self, mock_request):
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        response = self._agent.execute_operation(
            "http",
            "do_request",
            _HTTP_OPERATION,
            None,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "User-Agent": _HTTP_USER_AGENT,
            },
        )
        mock_response.assert_has_calls(
            [
                call.raise_for_status(),
                call.json(),
            ]
        )
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        self.assertEqual(expected_result, response.result.get(ATTRIBUTE_NAME_RESULT))

    @patch("requests.request")
    def test_http_request_with_params(self, mock_request):
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        operation = deepcopy(_HTTP_OPERATION)
        params = {
            "int_param": 23,
            "str_param": "abc",
        }
        operation["commands"][0]["kwargs"]["params"] = params
        response = self._agent.execute_operation(
            "http",
            "do_request",
            operation,
            _HTTP_CREDENTIALS,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": f"Bearer {_HTTP_CREDENTIALS['token']}",
                "User-Agent": _HTTP_USER_AGENT,
            },
            params=params,
        )

    @patch("requests.request")
    def test_http_request_with_verify_ssl(self, mock_request):
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        operation = deepcopy(_HTTP_OPERATION)
        operation["commands"][0]["kwargs"]["verify_ssl"] = True
        self._agent.execute_operation(
            "http",
            "do_request",
            operation,
            _HTTP_CREDENTIALS,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": f"Bearer {_HTTP_CREDENTIALS['token']}",
                "User-Agent": _HTTP_USER_AGENT,
            },
            verify=True,
        )

        operation["commands"][0]["kwargs"]["verify_ssl"] = False
        self._agent.execute_operation(
            "http",
            "do_request",
            operation,
            _HTTP_CREDENTIALS,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": f"Bearer {_HTTP_CREDENTIALS['token']}",
                "User-Agent": _HTTP_USER_AGENT,
            },
            verify=False,
        )

    @patch("requests.request")
    def test_http_request_failed(self, mock_request):
        mock_response = create_autospec(Response)
        mock_response.status_code = 404
        mock_response.text = "not found"
        mock_response.reason = "NOT FOUND"
        mock_request.return_value = mock_response
        mock_response.raise_for_status.side_effect = HTTPError(
            "failed",
            response=mock_response,
        )

        response = self._agent.execute_operation(
            "http",
            "do_request",
            _HTTP_OPERATION,
            _HTTP_CREDENTIALS,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": f"Bearer {_HTTP_CREDENTIALS['token']}",
                "User-Agent": _HTTP_USER_AGENT,
            },
        )
        mock_response.assert_has_calls(
            [
                call.raise_for_status(),
            ]
        )
        self.assertIsNotNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual(mock_response.text, response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual("HTTPError", response.result.get(ATTRIBUTE_NAME_ERROR_TYPE))
        self.assertEqual(
            {
                "status_code": 404,
                "reason": mock_response.reason,
            },
            response.result.get(ATTRIBUTE_NAME_ERROR_ATTRS),
        )

    @patch("requests.request")
    @patch("builtins.open", new_callable=mock_open)
    def test_http_request_with_ssl_options_ca_data(self, mock_file, mock_request):
        """Test that ssl_options with ca_data configures SSL verification with a cert file"""
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {"ok": True}
        mock_response.json.return_value = expected_result

        ca_data = "-----BEGIN CERTIFICATE-----\nMIIDtest\n-----END CERTIFICATE-----"
        credentials = {
            "token": "test_token",
            "ssl_options": {
                "ca_data": ca_data,
            },
        }

        operation = deepcopy(_HTTP_OPERATION)
        response = self._agent.execute_operation(
            "http",
            "do_request",
            operation,
            credentials,
        )

        # Verify the cert file was written
        mock_file.assert_called()
        write_calls = mock_file().write.call_args_list
        self.assertEqual(1, len(write_calls))
        self.assertEqual(ca_data, write_calls[0][0][0])

        # Verify request was made with verify pointing to a cert file path
        call_kwargs = mock_request.call_args[1]
        self.assertIn("verify", call_kwargs)
        self.assertTrue(call_kwargs["verify"].endswith("_http_ca.pem"))

        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        self.assertEqual(expected_result, response.result.get(ATTRIBUTE_NAME_RESULT))

    @patch("requests.request")
    def test_http_request_with_ssl_options_disabled(self, mock_request):
        """Test that ssl_options with disabled=True sets verify=False"""
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {"ok": True}
        mock_response.json.return_value = expected_result

        credentials = {
            "token": "test_token",
            "ssl_options": {
                "disabled": True,
            },
        }

        operation = deepcopy(_HTTP_OPERATION)
        response = self._agent.execute_operation(
            "http",
            "do_request",
            operation,
            credentials,
        )

        # Verify request was made with verify=False
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": "Bearer test_token",
                "User-Agent": _HTTP_USER_AGENT,
            },
            verify=False,
        )

        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        self.assertEqual(expected_result, response.result.get(ATTRIBUTE_NAME_RESULT))

    @patch("requests.request")
    @patch("builtins.open", new_callable=mock_open)
    def test_http_request_verify_ssl_overrides_ssl_options(
        self, mock_file, mock_request
    ):
        """Test that verify_ssl parameter takes precedence over ssl_options"""
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {"ok": True}
        mock_response.json.return_value = expected_result

        ca_data = "-----BEGIN CERTIFICATE-----\nMIIDtest\n-----END CERTIFICATE-----"
        credentials = {
            "token": "test_token",
            "ssl_options": {
                "ca_data": ca_data,
            },
        }

        # Set verify_ssl=False to override the ssl_options ca_data
        operation = deepcopy(_HTTP_OPERATION)
        operation["commands"][0]["kwargs"]["verify_ssl"] = False

        self._agent.execute_operation(
            "http",
            "do_request",
            operation,
            credentials,
        )

        # Verify request was made with verify=False (from verify_ssl param)
        # instead of the cert file path (from ssl_options)
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": "Bearer test_token",
                "User-Agent": _HTTP_USER_AGENT,
            },
            verify=False,
        )

    def test_http_client_ssl_options_none_credentials(self):
        """Test that HttpProxyClient handles None credentials gracefully"""
        client = HttpProxyClient(credentials=None)
        self.assertIsNone(client._ssl_verify)

    def test_http_client_ssl_options_empty(self):
        """Test that HttpProxyClient handles empty ssl_options gracefully"""
        client = HttpProxyClient(credentials={"token": "test"})
        self.assertIsNone(client._ssl_verify)

    def test_http_client_ssl_options_none_value(self):
        """Test that HttpProxyClient handles ssl_options=None gracefully"""
        client = HttpProxyClient(credentials={"token": "test", "ssl_options": None})
        self.assertIsNone(client._ssl_verify)
