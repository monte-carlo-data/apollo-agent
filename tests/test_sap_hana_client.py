import datetime
import json
import logging
from typing import (
    Iterable,
    List,
    Any,
    Optional,
)
from unittest import TestCase
from unittest.mock import Mock, call, patch

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.interfaces.lambda_function.json_log_formatter import JsonLogFormatter

# Flat (raw) credentials — what the CTP pipeline receives.
_SAP_HANA_FLAT_CREDENTIALS = {
    "host": "hana.example.com",
    "port": 39015,
    "user": "SYSTEM",
    "password": "supersecure",
    "db_name": "HXE",
}

# Expected connect() kwargs after CTP transforms flat credentials.
_SAP_HANA_EXPECTED_CONNECT_ARGS = {
    "address": "hana.example.com",
    "port": 39015,
    "user": "SYSTEM",
    "password": "supersecure",
    "databaseName": "HXE",
}

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


class SAPHanaClientTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor
        self.maxDiff = None

    @patch("hdbcli.dbapi.connect")
    def test_query(self, mock_connect):
        query = "SELECT name, value FROM table LIMIT ? OFFSET ?"  # noqa
        args = [0, 2]
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
            ["name", 1, None, None, None, None, None],
            ["value", 1, None, None, None, None, None],
        ]
        self._test_run_query(
            mock_connect, query, args, expected_data, expected_description
        )

    def _test_run_query(
        self,
        mock_connect: Mock,
        query: str,
        query_args: Optional[Iterable[Any]],
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
                        query_args,
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
            "sap-hana",
            "run_query",
            operation_dict,
            _SAP_HANA_FLAT_CREDENTIALS,
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

        # After CTP the connect() call receives transformed args: host→address, db_name→databaseName.
        mock_connect.assert_called_with(**_SAP_HANA_EXPECTED_CONNECT_ARGS)
        self._mock_cursor.execute.assert_has_calls(
            [
                call(query, query_args),
            ]
        )
        self._mock_cursor.description.assert_called()
        self._mock_cursor.rowcount.assert_called()

        expected_data = self._serialized_data(data)
        self.assertTrue("all_results" in result)
        self.assertEqual(expected_data, result["all_results"])

        expected_description = self._serialized_description(description)
        self.assertTrue("description" in result)
        self.assertEqual(expected_description, result["description"])

        self.assertTrue("rowcount" in result)
        self.assertEqual(expected_rows, result["rowcount"])

    @classmethod
    def _serialized_data(cls, data: List) -> List:
        return [cls._serialized_row(v) for v in data]

    @classmethod
    def _serialized_description(cls, description: List) -> List:
        return [cls._serialized_col(v) for v in description]

    @classmethod
    def _serialized_row(cls, row: List) -> List:
        return [cls._serialized_value(v) for v in row]

    @classmethod
    def _serialized_col(cls, col: List) -> List:
        return [cls._serialized_value(v) for v in col]

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


class _ListHandler(logging.Handler):
    def __init__(self, records: list):
        super().__init__()
        self._records = records

    def emit(self, record: logging.LogRecord) -> None:
        self._records.append(record)


class SapHanaCtpCredentialSafetyTests(TestCase):
    """Verify that no credential values appear in agent error responses (DC→Sentry path)."""

    _HOST = "hana-cluster.example.com"
    _USER = "SYSTEM"
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
            "port": 39015,
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

    @patch("hdbcli.dbapi.connect")
    def test_missing_required_field_error_is_actionable_and_safe(self, mock_connect):
        """CTP validation failure: error names the missing field, no credentials in response."""
        creds = self._flat_creds()
        del creds["host"]

        response = self._agent.execute_operation(
            "sap-hana", "run_query", _OPERATION, creds
        )

        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result[ATTRIBUTE_NAME_ERROR]
        # CTP schema uses driver-native name 'address' — error should reference it
        self.assertIn("address", error, "error should name the missing field")
        mock_connect.assert_not_called()
        self._assert_no_credential_leak(response)
        self._assert_no_password_in_logs()

    @patch("hdbcli.dbapi.connect")
    def test_connect_failure_is_actionable_and_safe(self, mock_connect):
        """Driver connection failure: hostname visible in error, no password in response."""
        mock_connect.side_effect = Exception(
            f"Connection failed to host '{self._HOST}' port 39015: Connection refused"
        )

        response = self._agent.execute_operation(
            "sap-hana", "run_query", _OPERATION, self._flat_creds()
        )

        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result[ATTRIBUTE_NAME_ERROR]
        self.assertIn(self._HOST, error, "error should show where connection failed")
        self._assert_no_credential_leak(response)
        self._assert_no_password_in_logs()

    @patch("hdbcli.dbapi.connect")
    def test_auth_failure_is_actionable_and_safe(self, mock_connect):
        """Auth failure: error is actionable, password never in response."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        mock_cursor.execute.side_effect = Exception(
            f"authentication failed for user '{self._USER}': invalid credentials"
        )

        response = self._agent.execute_operation(
            "sap-hana", "run_query", _OPERATION, self._flat_creds()
        )

        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        serialized = json.dumps(response.result, default=str)
        self.assertNotIn(self._PASSWORD, serialized, "password leaked in response")
        self._assert_no_password_in_logs()
