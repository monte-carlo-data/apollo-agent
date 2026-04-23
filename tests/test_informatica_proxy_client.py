import json
import logging
from unittest import TestCase
from unittest.mock import MagicMock, patch

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.interfaces.lambda_function.json_log_formatter import JsonLogFormatter

# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

_API_BASE_URL = "https://na1.informaticacloud.com"
_SESSION_ID = "pre-resolved-session-abc123"

# connect_args produced by the resolve_informatica_session CTP transform —
# the proxy client never sees raw credentials.
_RESOLVED_CONNECT_ARGS = {
    "session_id": _SESSION_ID,
    "api_base_url": _API_BASE_URL,
}


def _make_api_mock(body: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = body
    resp.text = json.dumps(body)
    resp.raise_for_status.return_value = None
    return resp


def _make_operation(path: str, http_method: str = "GET", **kwargs) -> dict:
    return {
        "trace_id": "test",
        "skip_cache": True,
        "commands": [
            {
                "method": "do_http_request",
                "kwargs": {"path": path, "http_method": http_method, **kwargs},
            }
        ],
    }


# ---------------------------------------------------------------------------
# HTTP request tests
# ---------------------------------------------------------------------------


class InformaticaHttpTests(TestCase):
    """Tests for InformaticaProxyClient do_http_request behavior.

    The proxy client receives pre-resolved session credentials from the CTP
    pipeline — there is no login call here. Login logic lives in the
    resolve_informatica_session transform (see tests/ctp/test_informatica_ctp.py).
    """

    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())

    @patch("requests.request")
    def test_do_http_request_constructs_url_from_api_base_url(self, mock_request):
        """API calls use api_base_url from connect_args, not any login URL."""
        mock_request.return_value = _make_api_mock({"items": []})

        self._agent.execute_operation(
            "informatica",
            "http_request",
            _make_operation("/api/v2/workflow"),
            {"connect_args": _RESOLVED_CONNECT_ARGS},
        )

        api_url = mock_request.call_args[0][1]
        self.assertTrue(
            api_url.startswith(_API_BASE_URL),
            f"Expected URL to start with {_API_BASE_URL!r}, got {api_url!r}",
        )
        self.assertIn("/api/v2/workflow", api_url)

    @patch("requests.request")
    def test_do_http_request_sends_ic_session_id_header(self, mock_request):
        """All API calls use icSessionId header regardless of how the session was obtained.

        This is coupled to V2 API usage, not the auth method. Acts as a regression
        guard for the intentional icSessionId-over-INFA-SESSION-ID behavior.
        """
        mock_request.return_value = _make_api_mock({"result": "ok"})

        self._agent.execute_operation(
            "informatica",
            "http_request",
            _make_operation("/api/v2/workflow"),
            {"connect_args": _RESOLVED_CONNECT_ARGS},
        )

        headers = mock_request.call_args[1]["headers"]
        self.assertIn("icSessionId", headers, "icSessionId header must always be sent")
        self.assertNotIn(
            "INFA-SESSION-ID", headers, "INFA-SESSION-ID must not be used (V2 API path)"
        )
        self.assertEqual(headers["icSessionId"], _SESSION_ID)

    @patch("requests.request")
    def test_do_http_request_passes_params(self, mock_request):
        """Query params are forwarded to the API call."""
        mock_request.return_value = _make_api_mock({"items": []})

        self._agent.execute_operation(
            "informatica",
            "http_request",
            _make_operation(
                "/api/v2/activity/activityLog",
                params={"taskId": "t1", "offset": 0, "rowLimit": 100},
            ),
            {"connect_args": _RESOLVED_CONNECT_ARGS},
        )

        self.assertEqual(
            mock_request.call_args[1].get("params"),
            {"taskId": "t1", "offset": 0, "rowLimit": 100},
        )

    @patch("requests.request")
    def test_do_http_request_post_with_payload(self, mock_request):
        """POST requests forward the JSON payload."""
        mock_request.return_value = _make_api_mock({"id": "job-123"})

        response = self._agent.execute_operation(
            "informatica",
            "http_request",
            _make_operation(
                "/api/v2/jobs", http_method="POST", payload={"name": "new-job"}
            ),
            {"connect_args": _RESOLVED_CONNECT_ARGS},
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual(mock_request.call_args[0][0], "POST")
        self.assertEqual(mock_request.call_args[1].get("json"), {"name": "new-job"})

    @patch("requests.request")
    def test_http_error_surfaced_as_http_error_type(self, mock_request):
        """HTTP errors on API calls are surfaced with HTTPError type."""
        from requests import HTTPError

        mock_fail = MagicMock()
        mock_fail.status_code = 404
        mock_fail.text = "Not Found"
        mock_fail.reason = "Not Found"
        mock_fail.raise_for_status.side_effect = HTTPError(response=mock_fail)
        mock_request.return_value = mock_fail

        response = self._agent.execute_operation(
            "informatica",
            "http_request",
            _make_operation("/api/v2/nonexistent"),
            {"connect_args": _RESOLVED_CONNECT_ARGS},
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
    """Verify that session credentials do not leak into responses or log output."""

    _SESSION_ID = "super-secret-session-token-xyz"
    _API_BASE_URL = "https://na1.informaticacloud.com"

    _CONNECT_ARGS = {
        "session_id": _SESSION_ID,
        "api_base_url": _API_BASE_URL,
    }

    _OPERATION = {
        "trace_id": "safety-test",
        "skip_cache": True,
        "commands": [
            {"method": "do_http_request", "kwargs": {"path": "/api/v2/workflow"}}
        ],
    }

    def setUp(self):
        self._agent = Agent(LoggingUtils())
        self._log_records = []
        self._log_handler = _ListHandler(self._log_records)
        logging.getLogger().addHandler(self._log_handler)

    def tearDown(self):
        logging.getLogger().removeHandler(self._log_handler)

    def test_missing_credentials_gives_actionable_error(self):
        """Empty connect_args (no username/password, no jwt_token, no session_id) →
        actionable error from the CTP pipeline, not a cryptic traceback."""
        response = self._agent.execute_operation(
            "informatica",
            "http_request",
            self._OPERATION,
            {"connect_args": {}},
        )
        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        # Error must name what's missing so the user knows what to fix.
        error = response.result[ATTRIBUTE_NAME_ERROR]
        self.assertTrue(
            any(token in error for token in ("username", "jwt_token", "session_id")),
            f"Error must name the missing credential input, got: {error!r}",
        )

    @patch("requests.request")
    def test_log_output_does_not_leak_session_token(self, mock_request):
        """JsonLogFormatter (Datadog/Lambda path) never emits the session token."""
        from requests import HTTPError

        mock_fail = MagicMock()
        mock_fail.status_code = 403
        mock_fail.text = "Forbidden"
        mock_fail.raise_for_status.side_effect = HTTPError(response=mock_fail)
        mock_request.return_value = mock_fail

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
                self._SESSION_ID,
                output,
                f"session token leaked in log record: {output}",
            )
