import base64
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
from psycopg2.errors import InsufficientPrivilege  # noqa

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.agent.logging_utils import LoggingUtils

_MYSQL_CREDENTIALS = {
    "host": "www.test.com",
    "user": "u",
    "password": "p",
    "port": 3306,
}

# Expected connect_args after CTP passes through _MYSQL_CREDENTIALS unchanged (DC path).
_EXPECTED_MYSQL_CONNECT_ARGS = {
    "host": "www.test.com",
    "user": "u",
    "password": "p",
    "port": 3306,
}


class MySqlClientTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor
        self.maxDiff = None

    @patch("pymysql.connect")
    def test_query(self, mock_connect):
        query = "SELECT name, value FROM table LIMIT %s OFFSET %s"  # noqa
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
            ["name", "string", None, None, None, None, None],
            ["value", "float", None, None, None, None, None],
        ]
        self._test_run_query(
            mock_connect, query, args, expected_data, expected_description
        )

    @patch("pymysql.connect")
    def test_datatypes_query(self, mock_connect):
        query = "SELECT name, created_date, updated_datetime FROM table"  # noqa
        data = [
            [
                "name_1",
                datetime.date.fromisoformat("2023-11-01"),
                datetime.datetime.fromisoformat("2023-11-01T10:59:00"),
                b"\x01",
            ],
        ]
        description = [
            ["name", "string", None, None, None, None, None],
            ["created_date", "date", None, None, None, None, None],
            ["updated_datetime", "date", None, None, None, None, None],
            ["active", "bit", None, None, None, None, None],
        ]
        self._test_run_query(mock_connect, query, None, data, description)

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
            "mysql",
            "run_query",
            operation_dict,
            {
                "connect_args": _MYSQL_CREDENTIALS,
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

        mock_connect.assert_called_with(**_EXPECTED_MYSQL_CONNECT_ARGS)
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
        elif isinstance(value, bytes):
            return {
                "__type__": "bytes",
                "__data__": base64.b64encode(value).decode("utf-8"),
            }
        else:
            return value


class _ListHandler(logging.Handler):
    def __init__(self, records):
        super().__init__()
        self._records = records

    def emit(self, record):
        self._records.append(record)


class MysqlCtpCredentialSafetyTests(TestCase):
    """CTP connection errors must be actionable without leaking credentials."""

    _HOST = "db.example.com"
    _USER = "svc_account@example.com"
    _PASSWORD = "s3cr3t_p@ssw0rd!"

    _OPERATION = {
        "trace_id": "ctp-safety-test",
        "skip_cache": True,
        "commands": [
            {"method": "cursor", "store": "_cursor"},
            {"target": "_cursor", "method": "execute", "args": ["SELECT 1", None]},
        ],
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
        self.assertNotIn(self._PASSWORD, serialized, "password leaked in response")
        self.assertNotIn(self._USER, serialized, "username leaked in response")

    @patch("pymysql.connect")
    def test_connect_failure_is_actionable_and_safe(self, mock_connect):
        """Connection failure exposes the hostname but not the password."""
        mock_connect.side_effect = Exception(
            f"Can't connect to MySQL server on '{self._HOST}'"
        )
        response = self._agent.execute_operation(
            "mysql",
            "run_query",
            self._OPERATION,
            {
                "host": self._HOST,
                "port": "3306",
                "user": self._USER,
                "password": self._PASSWORD,
            },
        )
        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn(self._HOST, error)
        self._assert_no_credential_leak(response)

    @patch("pymysql.connect")
    def test_auth_failure_is_actionable_and_safe(self, mock_connect):
        """Auth failure surfaces a useful error without leaking credentials."""
        mock_connect.side_effect = Exception("1045 (28000): Access denied for user")
        response = self._agent.execute_operation(
            "mysql",
            "run_query",
            self._OPERATION,
            {
                "host": self._HOST,
                "port": "3306",
                "user": self._USER,
                "password": self._PASSWORD,
            },
        )
        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn("1045", error)
        self._assert_no_credential_leak(response)

    @patch("pymysql.connect")
    def test_log_output_does_not_leak_credentials(self, mock_connect):
        """JsonLogFormatter (Datadog/Lambda path) never emits the password."""
        from apollo.interfaces.lambda_function.json_log_formatter import (
            JsonLogFormatter,
        )

        mock_connect.side_effect = Exception(f"Can't connect to {self._HOST}")
        self._agent.execute_operation(
            "mysql",
            "run_query",
            self._OPERATION,
            {
                "host": self._HOST,
                "port": "3306",
                "user": self._USER,
                "password": self._PASSWORD,
            },
        )
        formatter = JsonLogFormatter()
        for record in self._log_records:
            output = formatter.format(record)
            self.assertNotIn(self._PASSWORD, output)
