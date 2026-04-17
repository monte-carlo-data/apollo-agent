import json
import logging
from unittest import TestCase
from unittest.mock import MagicMock, patch

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.interfaces.lambda_function.json_log_formatter import JsonLogFormatter

# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

_V2_CONNECT_ARGS = {
    "username": "testuser",
    "password": "s3cr3t_p@ss!",
    "informatica_auth": "v2",
}

_V3_CONNECT_ARGS = {
    "username": "testuser",
    "password": "s3cr3t_p@ss!",
    "informatica_auth": "v3",
}

_API_BASE_URL_V2 = "https://na1.informaticacloud.com"
_API_BASE_URL_V3 = "https://eu1.informaticacloud.com"

_V2_LOGIN_BODY = {
    "serverUrl": _API_BASE_URL_V2,
    "icSessionId": "v2-session-abc123",
}

_V3_LOGIN_BODY = {
    "products": [
        {
            "name": "Integration Cloud",
            "baseApiUrl": _API_BASE_URL_V3,
        },
        {
            "name": "Other Service",
            "baseApiUrl": "https://other.example.com",
        },
    ],
    "userInfo": {"sessionId": "v3-session-xyz789"},
}


def _make_mock_response(body: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = body
    resp.text = json.dumps(body)
    return resp


def _make_login_mock(body: dict) -> MagicMock:
    resp = _make_mock_response(body)
    resp.raise_for_status.return_value = None
    return resp


def _make_api_mock(body: dict) -> MagicMock:
    resp = _make_mock_response(body)
    resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# HTTP request tests
# ---------------------------------------------------------------------------


class InformaticaHttpTests(TestCase):
    """Tests for InformaticaProxyClient login and do_http_request behavior."""

    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())

    @patch("requests.request")
    @patch("requests.post")
    def test_v2_login_success_and_do_http_request(self, mock_post, mock_request):
        """V2 login: login POST sent to v2 path, API calls use serverUrl from response."""
        mock_post.return_value = _make_login_mock(_V2_LOGIN_BODY)
        mock_request.return_value = _make_api_mock({"items": []})

        operation_dict = {
            "trace_id": "v2-test",
            "skip_cache": True,
            "commands": [
                {
                    "method": "do_http_request",
                    "kwargs": {
                        "path": "/mfnRunStatus/api/v1/Activity",
                        "http_method": "GET",
                    },
                }
            ],
        }

        response = self._agent.execute_operation(
            "informatica",
            "http_request",
            operation_dict,
            {"connect_args": _V2_CONNECT_ARGS},
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))

        # Login went to the V2 path
        mock_post.assert_called_once()
        login_url = mock_post.call_args[0][0]
        self.assertIn("/ma/api/v2/user/login", login_url)

        # API call used the API base URL from the login response, not the login URL
        mock_request.assert_called_once()
        api_url = mock_request.call_args[0][1]
        self.assertTrue(
            api_url.startswith(_API_BASE_URL_V2),
            f"Expected API URL to start with {_API_BASE_URL_V2!r}, got {api_url!r}",
        )
        self.assertIn("/mfnRunStatus/api/v1/Activity", api_url)

    @patch("requests.request")
    @patch("requests.post")
    def test_v3_login_success_and_do_http_request(self, mock_post, mock_request):
        """V3 login: login POST sent to v3 path, API base URL from Integration Cloud product."""
        mock_post.return_value = _make_login_mock(_V3_LOGIN_BODY)
        mock_request.return_value = _make_api_mock({"items": []})

        operation_dict = {
            "trace_id": "v3-test",
            "skip_cache": True,
            "commands": [
                {
                    "method": "do_http_request",
                    "kwargs": {
                        "path": "/mfnRunStatus/api/v1/Activity",
                        "http_method": "GET",
                    },
                }
            ],
        }

        response = self._agent.execute_operation(
            "informatica",
            "http_request",
            operation_dict,
            {"connect_args": _V3_CONNECT_ARGS},
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))

        # Login went to the V3 path
        login_url = mock_post.call_args[0][0]
        self.assertIn("/saas/public/core/v3/login", login_url)

        # API call used the Integration Cloud product's baseApiUrl
        api_url = mock_request.call_args[0][1]
        self.assertTrue(
            api_url.startswith(_API_BASE_URL_V3),
            f"Expected API URL to start with {_API_BASE_URL_V3!r}, got {api_url!r}",
        )

    @patch("requests.request")
    @patch("requests.post")
    def test_api_requests_use_base_url_from_login_response_not_login_url(
        self, mock_post, mock_request
    ):
        """API calls use the API base URL returned by login, not the login base URL."""
        login_base_url = "https://dm-us.informaticacloud.com"
        connect_args = {**_V2_CONNECT_ARGS, "base_url": login_base_url}

        mock_post.return_value = _make_login_mock(
            _V2_LOGIN_BODY
        )  # serverUrl differs from login URL
        mock_request.return_value = _make_api_mock({"result": "ok"})

        operation_dict = {
            "trace_id": "url-test",
            "skip_cache": True,
            "commands": [
                {
                    "method": "do_http_request",
                    "kwargs": {"path": "/v2/jobs", "http_method": "GET"},
                }
            ],
        }

        self._agent.execute_operation(
            "informatica",
            "http_request",
            operation_dict,
            {"connect_args": connect_args},
        )

        api_url = mock_request.call_args[0][1]
        # Must NOT start with the login base URL — must use serverUrl from login response
        self.assertFalse(
            api_url.startswith(login_base_url),
            f"API URL should use serverUrl from login response, not the login base URL. Got: {api_url!r}",
        )
        self.assertTrue(api_url.startswith(_API_BASE_URL_V2))

    @patch("requests.request")
    @patch("requests.post")
    def test_v3_session_uses_v2_auth_header(self, mock_post, mock_request):
        """V3-authenticated sessions use icSessionId (not INFA-SESSION-ID) for API calls.

        The DC currently calls only V2 API endpoints, which require the icSessionId header.
        This test acts as a regression guard for that intentional quirk.
        """
        mock_post.return_value = _make_login_mock(_V3_LOGIN_BODY)
        mock_request.return_value = _make_api_mock({"result": "ok"})

        operation_dict = {
            "trace_id": "header-test",
            "skip_cache": True,
            "commands": [
                {
                    "method": "do_http_request",
                    "kwargs": {"path": "/v2/jobs", "http_method": "GET"},
                }
            ],
        }

        self._agent.execute_operation(
            "informatica",
            "http_request",
            operation_dict,
            {"connect_args": _V3_CONNECT_ARGS},
        )

        headers = mock_request.call_args[1]["headers"]
        self.assertIn("icSessionId", headers, "V3 sessions must use icSessionId header")
        self.assertNotIn(
            "INFA-SESSION-ID", headers, "INFA-SESSION-ID must not be used (V2 API path)"
        )
        self.assertEqual(headers["icSessionId"], "v3-session-xyz789")

    @patch("requests.post")
    def test_login_failure_raises_error(self, mock_post):
        """Login failure surfaces as an error in the agent response."""
        from requests import HTTPError

        mock_fail = MagicMock()
        mock_fail.status_code = 401
        mock_fail.text = "Unauthorized"
        mock_fail.raise_for_status.side_effect = HTTPError(response=mock_fail)
        mock_post.return_value = mock_fail

        operation_dict = {
            "trace_id": "fail-test",
            "skip_cache": True,
            "commands": [{"method": "do_http_request", "kwargs": {"path": "/v2/jobs"}}],
        }

        response = self._agent.execute_operation(
            "informatica",
            "http_request",
            operation_dict,
            {"connect_args": _V2_CONNECT_ARGS},
        )

        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result[ATTRIBUTE_NAME_ERROR]
        self.assertIn("login failed", error.lower())

    @patch("requests.request")
    @patch("requests.post")
    def test_do_http_request_passes_params(self, mock_post, mock_request):
        """Query params are forwarded to the API call."""
        mock_post.return_value = _make_login_mock(_V2_LOGIN_BODY)
        mock_request.return_value = _make_api_mock({"items": []})

        operation_dict = {
            "trace_id": "params-test",
            "skip_cache": True,
            "commands": [
                {
                    "method": "do_http_request",
                    "kwargs": {
                        "path": "/v2/jobs",
                        "http_method": "GET",
                        "params": {"limit": 50, "offset": 0},
                    },
                }
            ],
        }

        self._agent.execute_operation(
            "informatica",
            "http_request",
            operation_dict,
            {"connect_args": _V2_CONNECT_ARGS},
        )

        self.assertEqual(
            mock_request.call_args[1].get("params"), {"limit": 50, "offset": 0}
        )

    @patch("requests.request")
    @patch("requests.post")
    def test_do_http_request_post_with_payload(self, mock_post, mock_request):
        """POST requests forward the JSON payload."""
        mock_post.return_value = _make_login_mock(_V2_LOGIN_BODY)
        mock_request.return_value = _make_api_mock({"id": "job-123"})

        operation_dict = {
            "trace_id": "post-test",
            "skip_cache": True,
            "commands": [
                {
                    "method": "do_http_request",
                    "kwargs": {
                        "path": "/v2/jobs",
                        "http_method": "POST",
                        "payload": {"name": "new-job", "type": "mapping"},
                    },
                }
            ],
        }

        response = self._agent.execute_operation(
            "informatica",
            "http_request",
            operation_dict,
            {"connect_args": _V2_CONNECT_ARGS},
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual(mock_request.call_args[0][0], "POST")
        self.assertEqual(
            mock_request.call_args[1].get("json"),
            {"name": "new-job", "type": "mapping"},
        )

    @patch("requests.request")
    @patch("requests.post")
    def test_http_error_handling(self, mock_post, mock_request):
        """HTTP errors on API calls are surfaced with HTTPError type."""
        from requests import HTTPError

        mock_post.return_value = _make_login_mock(_V2_LOGIN_BODY)

        mock_fail = MagicMock()
        mock_fail.status_code = 404
        mock_fail.text = "Not Found"
        mock_fail.reason = "Not Found"
        mock_fail.raise_for_status.side_effect = HTTPError(response=mock_fail)
        mock_request.return_value = mock_fail

        operation_dict = {
            "trace_id": "error-test",
            "skip_cache": True,
            "commands": [
                {"method": "do_http_request", "kwargs": {"path": "/v2/nonexistent"}}
            ],
        }

        response = self._agent.execute_operation(
            "informatica",
            "http_request",
            operation_dict,
            {"connect_args": _V2_CONNECT_ARGS},
        )

        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        self.assertEqual("HTTPError", response.result.get(ATTRIBUTE_NAME_ERROR_TYPE))


# ---------------------------------------------------------------------------
# Logging handler helper
# ---------------------------------------------------------------------------


class _ListHandler(logging.Handler):
    def __init__(self, records):
        super().__init__()
        self._records = records

    def emit(self, record):
        self._records.append(record)


# ---------------------------------------------------------------------------
# Credential safety tests
# ---------------------------------------------------------------------------


class InformaticaProxyClientSafetyTests(TestCase):
    """Verify that credentials do not leak into responses or log output."""

    _USERNAME = "svc_account@example.com"
    _PASSWORD = "s3cr3t_p@ssw0rd!"

    _CONNECT_ARGS = {
        "username": _USERNAME,
        "password": _PASSWORD,
        "informatica_auth": "v2",
    }

    _OPERATION = {
        "trace_id": "safety-test",
        "skip_cache": True,
        "commands": [{"method": "do_http_request", "kwargs": {"path": "/v2/jobs"}}],
    }

    def setUp(self):
        self._agent = Agent(LoggingUtils())
        self._log_records = []
        self._log_handler = _ListHandler(self._log_records)
        logging.getLogger().addHandler(self._log_handler)

    def tearDown(self):
        logging.getLogger().removeHandler(self._log_handler)

    def _assert_no_credential_leak(self, response) -> None:
        serialized = json.dumps(response.result, default=str)
        self.assertNotIn(
            self._PASSWORD, serialized, "password leaked in agent response"
        )

    def test_missing_username_is_actionable_and_safe(self):
        """Missing username produces an actionable error without leaking password."""
        connect_args = {"password": self._PASSWORD, "informatica_auth": "v2"}
        response = self._agent.execute_operation(
            "informatica",
            "http_request",
            self._OPERATION,
            {"connect_args": connect_args},
        )
        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result[ATTRIBUTE_NAME_ERROR]
        self.assertIn("username", error)
        self._assert_no_credential_leak(response)

    @patch("requests.post")
    def test_login_failure_is_actionable_and_safe(self, mock_post):
        """Login failure surfaces a useful error without leaking the password."""
        from requests import HTTPError

        mock_fail = MagicMock()
        mock_fail.status_code = 401
        mock_fail.text = "Unauthorized"
        mock_fail.raise_for_status.side_effect = HTTPError(response=mock_fail)
        mock_post.return_value = mock_fail

        response = self._agent.execute_operation(
            "informatica",
            "http_request",
            self._OPERATION,
            {"connect_args": self._CONNECT_ARGS},
        )

        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        self._assert_no_credential_leak(response)
        error = response.result[ATTRIBUTE_NAME_ERROR]
        self.assertIn("login failed", error.lower())

    @patch("requests.post")
    def test_log_output_does_not_leak_credentials(self, mock_post):
        """JsonLogFormatter (Datadog/Lambda path) never emits the password."""
        from requests import HTTPError

        mock_fail = MagicMock()
        mock_fail.status_code = 401
        mock_fail.text = "Unauthorized"
        mock_fail.raise_for_status.side_effect = HTTPError(response=mock_fail)
        mock_post.return_value = mock_fail

        self._agent.execute_operation(
            "informatica",
            "http_request",
            self._OPERATION,
            {"connect_args": self._CONNECT_ARGS},
        )

        formatter = JsonLogFormatter()
        for record in self._log_records:
            output = formatter.format(record)
            self.assertNotIn(
                self._PASSWORD, output, f"password leaked in log record: {output}"
            )
