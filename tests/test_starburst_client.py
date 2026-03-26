import datetime
import json
import logging
from logging.handlers import MemoryHandler
from typing import (
    List,
    Any,
    Optional,
)
from unittest import TestCase
from unittest.mock import (
    ANY,
    Mock,
    call,
    patch,
)

import trino.exceptions

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_EXCEPTION,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_STACK_TRACE,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.ctp.defaults.starburst_galaxy import (
    STARBURST_GALAXY_DEFAULT_CTP,
)
from apollo.integrations.ctp.registry import CtpRegistry
from apollo.integrations.db.starburst_proxy_client import StarburstProxyClient
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
_EXPECTED_STARBURST_CREDENTIALS = {
    "host": "example.starburst.io",
    "port": 443,  # CTP coerces string → int
    "http_scheme": "https",
    "catalog": "fizz",
    "schema": "buzz",
    "auth": ANY,
}
# starburst-enterprise is not CTP-registered, so port stays as the original string
_EXPECTED_STARBURST_ENTERPRISE_CREDENTIALS = {
    **_EXPECTED_STARBURST_CREDENTIALS,
    "port": "443",
}


class StarburstClientTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor

    @patch("trino.dbapi.connect")
    def test_query_starburst_galaxy(self, mock_connect):
        query = "SELECT idx, value FROM table"  # noqa
        expected_data = [
            [
                "name_1",
                1,
            ],
            [
                "name_2",
                22.2,
            ],
        ]
        expected_description = [
            ["idx", "integer", None, None, None, None, None],
            ["value", "float", None, None, None, None, None],
        ]
        self._test_run_query(
            mock_connect,
            query,
            expected_data,
            expected_description,
            connection_type="starburst-galaxy",
        )

    @patch("trino.dbapi.connect")
    def test_query_starburst_enterprise(self, mock_connect):
        query = "SELECT idx, value FROM table"  # noqa
        expected_data = [
            [
                "name_1",
                1,
            ],
            [
                "name_2",
                22.2,
            ],
        ]
        expected_description = [
            ["idx", "integer", None, None, None, None, None],
            ["value", "float", None, None, None, None, None],
        ]
        self._test_run_query(
            mock_connect,
            query,
            expected_data,
            expected_description,
            connection_type="starburst-enterprise",
            expected_connect_kwargs=_EXPECTED_STARBURST_ENTERPRISE_CREDENTIALS,
        )

    @patch("trino.dbapi.connect")
    def test_missing_credentials_raises_error(self, mock_connect):
        """Test that missing user or password raises an error"""
        operation_dict = {
            "trace_id": "1234",
            "skip_cache": True,
            "commands": [
                {"method": "cursor", "store": "_cursor"},
            ],
        }

        # Credentials without password
        credentials_no_password = {
            "host": "example.starburst.io",
            "port": "443",
            "http_scheme": "https",
            "catalog": "fizz",
            "schema": "buzz",
            "user": "foo",
        }

        response = self._agent.execute_operation(
            "starburst-galaxy",
            "run_query",
            operation_dict,
            {"connect_args": credentials_no_password},
        )

        self.assertIn("password", response.result.get(ATTRIBUTE_NAME_ERROR))
        mock_connect.assert_not_called()

        # Credentials without user
        credentials_no_user = {
            "host": "example.starburst.io",
            "port": "443",
            "http_scheme": "https",
            "catalog": "fizz",
            "schema": "buzz",
            "password": "bar",
        }

        response = self._agent.execute_operation(
            "starburst-galaxy",
            "run_query",
            operation_dict,
            {"connect_args": credentials_no_user},
        )

        self.assertIn("user", response.result.get(ATTRIBUTE_NAME_ERROR))
        mock_connect.assert_not_called()

    def _test_run_query(
        self,
        mock_connect: Mock,
        query: str,
        data: List,
        description: List,
        raise_exception: Optional[Exception] = None,
        expected_error_type: Optional[str] = None,
        connection_type: str = "starburst-galaxy",
        expected_connect_kwargs: Optional[dict] = None,
    ):
        operation_dict = {
            "trace_id": "1234",
            "skip_cache": True,
            "commands": [
                {"method": "cursor", "store": "_cursor"},
                {
                    "target": "_cursor",
                    "method": "execute",
                    "args": [
                        query,
                        None,
                    ],
                },
                {"target": "_cursor", "method": "fetchall", "store": "tmp_1"},
                {"target": "_cursor", "method": "description", "store": "tmp_2"},
                {"target": "_cursor", "method": "rowcount", "store": "tmp_3"},
                {
                    "target": "__utils",
                    "method": "build_dict",
                    "kwargs": {
                        "all_results": {"__reference__": "tmp_1"},
                        "description": {"__reference__": "tmp_2"},
                        "rowcount": {"__reference__": "tmp_3"},
                    },
                },
            ],
        }
        mock_connect.return_value = self._mock_connection

        expected_rows = len(data)

        if raise_exception:
            self._mock_cursor.execute.side_effect = raise_exception
        self._mock_cursor.fetchall.return_value = data
        self._mock_cursor.description.return_value = description
        self._mock_cursor.rowcount.return_value = expected_rows

        response = self._agent.execute_operation(
            connection_type,
            "run_query",
            operation_dict,
            {
                "connect_args": _STARBURST_CREDENTIALS,
            },
        )

        if raise_exception:
            self.assertEqual(
                str(raise_exception), response.result.get(ATTRIBUTE_NAME_ERROR)
            )
            self.assertEqual(
                expected_error_type, response.result.get(ATTRIBUTE_NAME_ERROR_TYPE)
            )
            return

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        result = response.result.get(ATTRIBUTE_NAME_RESULT)

        mock_connect.assert_called_with(
            **(expected_connect_kwargs or _EXPECTED_STARBURST_CREDENTIALS)
        )
        self._mock_cursor.execute.assert_has_calls(
            [
                call(query, None),
            ]
        )
        self._mock_cursor.description.assert_called()
        self._mock_cursor.rowcount.assert_called()

        expected_data = self._serialized_data(data)
        self.assertTrue("all_results" in result)
        self.assertEqual(expected_data, result["all_results"])

        self.assertTrue("description" in result)
        self.assertEqual(description, result["description"])

        self.assertTrue("rowcount" in result)
        self.assertEqual(expected_rows, result["rowcount"])

    @classmethod
    def _serialized_data(cls, data: List) -> List:
        return [cls._serialized_row(v) for v in data]

    @classmethod
    def _serialized_row(cls, row: List) -> List:
        return [cls._serialized_value(v) for v in row]

    @classmethod
    def _serialized_value(cls, value: Any) -> Any:
        if isinstance(value, datetime.datetime):
            return {
                "__type__": "datetime",
                "__data__": value.isoformat(),
            }
        elif isinstance(value, datetime.date):
            return {
                "__type__": "date",
                "__data__": value.isoformat(),
            }
        else:
            return value


class StarburstGalaxyCredentialShapeTests(TestCase):
    """Verify StarburstProxyClient __init__ accepts both DC-style and CTP-resolved credentials.

    DC path (today): DC plugin builds connect_args with all required fields and sends them to
    the agent. Port arrives as a string; user/password are converted to BasicAuthentication.

    CTP path (after Phase 2): flat credentials go through CTP, which converts port to int and
    hard-codes http_scheme before StarburstProxyClient is created.

    In both paths trino.dbapi.connect must receive the same effective arguments.
    """

    _HOST = "example.starburst.io"
    _PORT_STR = "443"
    _PORT_INT = 443
    _USER = "foo"
    _PASSWORD = "bar"

    def _dc_creds(self, **extra_connect_args):
        """Build DC-style credentials: connect_args with all required fields."""
        return {
            "connect_args": {
                "host": self._HOST,
                "port": self._PORT_STR,
                "http_scheme": "https",
                "user": self._USER,
                "password": self._PASSWORD,
                **extra_connect_args,
            }
        }

    def _ctp_creds(self, **flat_kwargs):
        """Build CTP-resolved credentials from flat input via the registry."""
        return CtpRegistry.resolve(
            "starburst-galaxy",
            {
                "host": self._HOST,
                "port": self._PORT_STR,
                "user": self._USER,
                "password": self._PASSWORD,
                **flat_kwargs,
            },
        )

    @patch("trino.dbapi.connect")
    def test_dc_path(self, mock_connect):
        """DC sends connect_args — port stays as string, user/password become BasicAuthentication."""
        mock_connect.return_value = Mock()
        StarburstProxyClient(credentials=self._dc_creds(), platform="test")

        kwargs = mock_connect.call_args.kwargs
        self.assertEqual(self._HOST, kwargs["host"])
        self.assertEqual(self._PORT_STR, kwargs["port"])  # string passes through as-is
        self.assertEqual("https", kwargs["http_scheme"])
        self.assertNotIn("user", kwargs)
        self.assertNotIn("password", kwargs)
        self.assertIn("auth", kwargs)

    @patch("trino.dbapi.connect")
    def test_ctp_path(self, mock_connect):
        """CTP resolves flat credentials — port is converted to int, http_scheme hard-coded."""
        mock_connect.return_value = Mock()
        StarburstProxyClient(credentials=self._ctp_creds(), platform="test")

        kwargs = mock_connect.call_args.kwargs
        self.assertEqual(self._HOST, kwargs["host"])
        self.assertEqual(self._PORT_INT, kwargs["port"])  # CTP converts to int
        self.assertEqual("https", kwargs["http_scheme"])
        self.assertNotIn("user", kwargs)
        self.assertNotIn("password", kwargs)
        self.assertIn("auth", kwargs)


class StarburstCtpCredentialSafetyTests(TestCase):
    """Verify that CTP pipeline failure paths do not leak credential values.

    Error messages must be actionable (say what went wrong) but must never
    include the password or username that were supplied in credentials.
    This matters for both the value returned in the agent response AND the
    exception message that lands in server-side logs via logger.exception().
    """

    _USER = "mc-service@org.galaxy.starburst.io"
    _PASSWORD = "sup3r_s3cr3t_p@ssw0rd"

    def _base_credentials(self, **overrides) -> dict:
        creds = {
            "host": "cluster.trino.galaxy.starburst.io",
            "port": "443",
            "http_scheme": "https",
            "user": self._USER,
            "password": self._PASSWORD,
        }
        creds.update(overrides)
        return {"connect_args": creds}

    def _assert_no_credential_leak(self, response) -> None:
        """Assert neither the password nor the username appears anywhere in the response."""
        # Serialize the whole result dict so we catch leaks in error, exception, and stack trace
        serialized = json.dumps(response.result, default=str)
        self.assertNotIn(self._PASSWORD, serialized, "password leaked in response")
        self.assertNotIn(self._USER, serialized, "username leaked in response")

    def _simple_operation(self) -> dict:
        return {
            "trace_id": "ctp-safety-test",
            "skip_cache": True,
            "commands": [{"method": "cursor", "store": "_cursor"}],
        }

    # ------------------------------------------------------------------
    # CTP pipeline failure paths
    # ------------------------------------------------------------------

    def test_missing_host_error_is_actionable_and_does_not_leak(self):
        """CTP mapper raises when 'host' is missing; error names the field, not the value."""
        agent = Agent(LoggingUtils())
        response = agent.execute_operation(
            "starburst-galaxy",
            "run_query",
            self._simple_operation(),
            {
                "connect_args": {
                    "port": "443",
                    "user": self._USER,
                    "password": self._PASSWORD,
                }
            },
        )

        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        # Actionable: tells the caller what field is missing
        self.assertIn("host", error)
        # Safe: credentials do not appear
        self._assert_no_credential_leak(response)

    @patch("trino.dbapi.connect")
    def test_connect_failure_does_not_leak_credentials(self, mock_connect):
        """When trino.dbapi.connect raises, the error must not contain the password."""
        mock_connect.side_effect = trino.exceptions.TrinoConnectionError(
            "HTTPSConnectionPool(host='cluster.trino.galaxy.starburst.io', port=443): "
            "Max retries exceeded with url: /v1/statement"
        )

        agent = Agent(LoggingUtils())
        response = agent.execute_operation(
            "starburst-galaxy",
            "run_query",
            self._simple_operation(),
            self._base_credentials(),
        )

        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        self._assert_no_credential_leak(response)
        # Actionable: the host is present in the error so callers know where it failed
        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn("cluster.trino.galaxy.starburst.io", error)

    @patch("trino.dbapi.connect")
    def test_auth_error_does_not_leak_password(self, mock_connect):
        """When cursor.execute raises TrinoAuthError (e.g. 401), the password must not appear.

        The error message may reference the username (that's acceptable — it comes from the
        server's 401 response), but the password must never be exposed.
        """
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        mock_cursor.execute.side_effect = trino.exceptions.TrinoAuthError(
            f"Error authenticating with Trino: [401] Unauthorized for user '{self._USER}'"
        )

        operation = {
            "trace_id": "auth-test",
            "skip_cache": True,
            "commands": [
                {"method": "cursor", "store": "_cursor"},
                {"target": "_cursor", "method": "execute", "args": ["SELECT 1", None]},
            ],
        }
        agent = Agent(LoggingUtils())
        response = agent.execute_operation(
            "starburst-galaxy",
            "run_query",
            operation,
            self._base_credentials(),
        )

        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        # Password must never leak — even if username appears (from the server's 401 body)
        serialized = json.dumps(response.result, default=str)
        self.assertNotIn(self._PASSWORD, serialized, "password leaked in response")
        # Actionable: the 401 status code is present so the caller knows it's an auth problem
        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn("401", error)


class StarburstLogCredentialSafetyTests(TestCase):
    """Verify that logger.exception() calls triggered by connection failures
    do not leak credential values into log output or Datadog via JsonLogFormatter.

    Standard Python logging does not capture local variable values (unlike Sentry SDK),
    so the risk is limited to exception messages. These tests verify that:
    1. Exception messages from Trino/CTP do not contain the password.
    2. JsonLogFormatter.format() (the Lambda log formatter that ships to Datadog)
       does not produce output containing the password.
    """

    _USER = "mc-service@org.galaxy.starburst.io"
    _PASSWORD = "sup3r_s3cr3t_p@ssw0rd"

    def setUp(self):
        self._agent = Agent(LoggingUtils())
        # Buffer captures all log records emitted during the test
        self._buffer = []
        self._handler = _ListHandler(self._buffer)
        self._handler.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(self._handler)

    def tearDown(self):
        logging.getLogger().removeHandler(self._handler)

    def _base_credentials(self) -> dict:
        return {
            "connect_args": {
                "host": "cluster.trino.galaxy.starburst.io",
                "port": "443",
                "http_scheme": "https",
                "user": self._USER,
                "password": self._PASSWORD,
            }
        }

    def _simple_operation(self) -> dict:
        return {
            "trace_id": "log-safety-test",
            "skip_cache": True,
            "commands": [{"method": "cursor", "store": "_cursor"}],
        }

    def _assert_no_password_in_log_records(self):
        """Assert the password does not appear in any captured log record's message
        or exception string — i.e., it was never passed to logger.exception()."""
        formatter = logging.Formatter("%(message)s")
        for record in self._buffer:
            formatted_msg = formatter.format(record)
            self.assertNotIn(
                self._PASSWORD,
                formatted_msg,
                "password leaked into log record message",
            )
            if record.exc_info and record.exc_info[1]:
                exc_str = str(record.exc_info[1])
                self.assertNotIn(
                    self._PASSWORD,
                    exc_str,
                    f"password leaked in exception message: {exc_str[:200]}",
                )

    def _assert_no_password_in_json_formatter_output(self):
        """Assert JsonLogFormatter (used in Lambda/Datadog path) does not emit the
        password in its JSON output — either raw or after standard_redact processing."""
        json_formatter = JsonLogFormatter()
        for record in self._buffer:
            try:
                output = json_formatter.format(record)
            except Exception:
                continue
            self.assertNotIn(
                self._PASSWORD,
                output,
                "password survived JsonLogFormatter redaction",
            )

    @patch("trino.dbapi.connect")
    def test_connect_failure_does_not_leak_password_in_logs(self, mock_connect):
        """When trino.dbapi.connect raises, logger.exception() must not log the password."""
        mock_connect.side_effect = trino.exceptions.TrinoConnectionError(
            "HTTPSConnectionPool(host='cluster.trino.galaxy.starburst.io', port=443): "
            "Max retries exceeded with url: /v1/statement"
        )

        self._agent.execute_operation(
            "starburst-galaxy",
            "run_query",
            self._simple_operation(),
            self._base_credentials(),
        )

        self._assert_no_password_in_log_records()
        self._assert_no_password_in_json_formatter_output()

    @patch("trino.dbapi.connect")
    def test_auth_failure_does_not_leak_password_in_logs(self, mock_connect):
        """When cursor.execute raises TrinoAuthError, logger calls must not log the password."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        mock_cursor.execute.side_effect = trino.exceptions.TrinoAuthError(
            f"Error authenticating with Trino: [401] Unauthorized for user '{self._USER}'"
        )

        operation = {
            "trace_id": "log-auth-test",
            "skip_cache": True,
            "commands": [
                {"method": "cursor", "store": "_cursor"},
                {"target": "_cursor", "method": "execute", "args": ["SELECT 1", None]},
            ],
        }

        self._agent.execute_operation(
            "starburst-galaxy",
            "run_query",
            operation,
            self._base_credentials(),
        )

        self._assert_no_password_in_log_records()
        self._assert_no_password_in_json_formatter_output()

    def test_missing_host_does_not_leak_password_in_logs(self):
        """When CTP pipeline fails (missing required field), logger must not log credentials."""
        self._agent.execute_operation(
            "starburst-galaxy",
            "run_query",
            self._simple_operation(),
            {
                "connect_args": {
                    "port": "443",
                    "user": self._USER,
                    "password": self._PASSWORD,
                }
            },
        )

        self._assert_no_password_in_log_records()
        self._assert_no_password_in_json_formatter_output()


class _ListHandler(logging.Handler):
    """Minimal log handler that appends every LogRecord to a list."""

    def __init__(self, records: list):
        super().__init__()
        self._records = records

    def emit(self, record: logging.LogRecord) -> None:
        self._records.append(record)
