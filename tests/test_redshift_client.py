import datetime
import json
import logging
from typing import List, Any, Optional
from unittest import TestCase
from unittest.mock import Mock, call, patch
from psycopg2.errors import InsufficientPrivilege  # noqa

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.interfaces.lambda_function.json_log_formatter import JsonLogFormatter

# Flat (raw) credentials — what the CTP pipeline receives from DC in the future,
# or what test_ctp_local.sh sends on the CTP path.
_RS_FLAT_CREDENTIALS = {
    "host": "www.test.com",
    "user": "u",
    "password": "p",
    "port": "5439",
    "db_name": "db1",
}

# Expected connect() kwargs after CTP transforms flat credentials.
_RS_EXPECTED_CONNECT_ARGS = {
    "host": "www.test.com",
    "port": 5439,
    "dbname": "db1",
    "user": "u",
    "password": "p",
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}


class RedshiftClientTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor

    @patch("psycopg2.connect")
    def test_query(self, mock_connect):
        query = "SELECT name, value FROM table"  # noqa
        expected_data = [
            [
                "name_1",
                11.1,
            ],
            [
                "name_2",
                22.2,
            ],
        ]
        expected_description = [
            ["name", "string", None, None, None, None, None],
            ["value", "float", None, None, None, None, None],
        ]
        self._test_run_query(mock_connect, query, expected_data, expected_description)

    @patch("psycopg2.connect")
    def test_datetime_query(self, mock_connect):
        query = "SELECT name, created_date, updated_datetime FROM table"  # noqa
        data = [
            [
                "name_1",
                datetime.date.fromisoformat("2023-11-01"),
                datetime.datetime.fromisoformat("2023-11-01T10:59:00"),
            ],
        ]
        description = [
            ["name", "string", None, None, None, None, None],
            ["created_date", "date", None, None, None, None, None],
            ["updated_datetime", "date", None, None, None, None, None],
        ]
        self._test_run_query(mock_connect, query, data, description)

    @patch("psycopg2.connect")
    def test_privilege_error(self, mock_connect):
        query = ""
        data = []
        description = []
        self._test_run_query(
            mock_connect,
            query,
            data,
            description,
            raise_exception=InsufficientPrivilege("insufficient privilege"),
            expected_error_type="InsufficientPrivilege",
        )

    def _test_run_query(
        self,
        mock_connect: Mock,
        query: str,
        data: List,
        description: List,
        raise_exception: Optional[Exception] = None,
        expected_error_type: Optional[str] = None,
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
            "redshift",
            "run_query",
            operation_dict,
            _RS_FLAT_CREDENTIALS,
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

        # After CTP the connect() call receives transformed args: port as int,
        # db_name → dbname, and hardcoded keepalives from the field_map.
        mock_connect.assert_called_with(**_RS_EXPECTED_CONNECT_ARGS)
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

        self._mock_connection.close.assert_called()

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


_OPERATION = {
    "trace_id": "ctp-safety-test",
    "skip_cache": True,
    "commands": [
        {"method": "cursor", "store": "_cursor"},
        {"target": "_cursor", "method": "execute", "args": ["SELECT 1", None]},
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


class _ListHandler(logging.Handler):
    def __init__(self, records: list):
        super().__init__()
        self._records = records

    def emit(self, record: logging.LogRecord) -> None:
        self._records.append(record)


class RedshiftCtpCredentialSafetyTests(TestCase):
    """Verify that no credential values appear in agent error responses (DC→Sentry path)."""

    _HOST = "redshift-cluster.abc123.us-east-1.redshift.amazonaws.com"
    _USER = "admin"
    _PASSWORD = "s3cr3t_p@ssw0rd!"

    def setUp(self):
        self._agent = Agent(LoggingUtils())
        self._log_records: list = []
        self._log_handler = _ListHandler(self._log_records)
        logging.getLogger().addHandler(self._log_handler)

    def tearDown(self):
        logging.getLogger().removeHandler(self._log_handler)

    def _flat_creds(self, **overrides):
        creds = {
            "host": self._HOST,
            "user": self._USER,
            "password": self._PASSWORD,
            "port": "5439",
            "db_name": "dev",
        }
        creds.update(overrides)
        return creds

    def _assert_no_credential_leak(self, response) -> None:
        """Serialize the entire result dict — catches leaks in error, exception, AND stack_trace."""
        serialized = json.dumps(response.result, default=str)
        self.assertNotIn(self._PASSWORD, serialized, "password leaked in response")
        self.assertNotIn(self._USER, serialized, "username leaked in response")

    def _assert_no_password_in_logs(self) -> None:
        json_formatter = JsonLogFormatter()
        for record in self._log_records:
            output = json_formatter.format(record)
            self.assertNotIn(self._PASSWORD, output, "password leaked in log output")

    @patch("psycopg2.connect")
    def test_missing_required_field_error_is_actionable_and_safe(self, mock_connect):
        """CTP validation failure: error names the missing field, no credentials in response."""
        creds = self._flat_creds()
        del creds["host"]

        response = self._agent.execute_operation(
            "redshift", "run_query", _OPERATION, creds
        )

        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result[ATTRIBUTE_NAME_ERROR]
        self.assertIn("host", error, "error should name the missing field")
        mock_connect.assert_not_called()
        self._assert_no_credential_leak(response)
        self._assert_no_password_in_logs()

    @patch("psycopg2.connect")
    def test_connect_failure_is_actionable_and_safe(self, mock_connect):
        """Driver connection failure: hostname visible in error, no password in response."""
        import psycopg2

        mock_connect.side_effect = psycopg2.OperationalError(
            f"could not connect to server: Connection refused\n"
            f'\tIs the server running on host "{self._HOST}" and accepting\n'
            f"\tTCP/IP connections on port 5439?"
        )

        response = self._agent.execute_operation(
            "redshift", "run_query", _OPERATION, self._flat_creds()
        )

        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result[ATTRIBUTE_NAME_ERROR]
        self.assertIn(self._HOST, error, "error should show where connection failed")
        self._assert_no_credential_leak(response)
        self._assert_no_password_in_logs()

    @patch("psycopg2.connect")
    def test_auth_failure_is_actionable_and_safe(self, mock_connect):
        """Auth failure: status visible in error, password never in response."""
        import psycopg2

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        mock_cursor.execute.side_effect = psycopg2.OperationalError(
            f'FATAL:  password authentication failed for user "{self._USER}"'
        )

        response = self._agent.execute_operation(
            "redshift", "run_query", _OPERATION, self._flat_creds()
        )

        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        serialized = json.dumps(response.result, default=str)
        self.assertNotIn(self._PASSWORD, serialized, "password leaked in response")
        self._assert_no_password_in_logs()
