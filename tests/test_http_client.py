from copy import deepcopy
from unittest import TestCase
from unittest.mock import create_autospec, patch, call

from requests import Response, HTTPError

from apollo.agent.agent import Agent
from apollo.agent.constants import (
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_ERROR_TYPE,
    ATTRIBUTE_NAME_ERROR_ATTRS,
)
from apollo.agent.logging_utils import LoggingUtils

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
