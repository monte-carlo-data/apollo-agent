import datetime
import decimal
from typing import List, Any, Optional
from unittest import TestCase
from unittest.mock import Mock, call, patch

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.agent.logging_utils import LoggingUtils

_CLICKHOUSE_CREDENTIALS = {
    "host": "localhost",
    "port": "8123",
    "username": "u",
    "password": "p",
    "database": "db1",
}


class ClickHouseClientTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor

    @patch("clickhouse_connect.dbapi.connect")
    def test_query(self, mock_connect: Mock):
        query = "SELECT id, name, price, xpos FROM table"  # noqa
        expected_data = [
            [
                111,
                "name_1",
                decimal.Decimal("11.400"),
                0.111429,
            ],
            [
                222,
                "name_2",
                decimal.Decimal("22.210"),
                0.254735,
            ],
        ]
        expected_description = [
            ["id", "UInt32", None, None, None, None, None],
            ["name", "String", None, None, None, None, None],
            ["price", "Decimal(18, 3)", None, None, None, None, None],
            ["xpos", "Float64", None, None, None, None, None],
        ]
        self._test_run_query(mock_connect, query, expected_data, expected_description)

    @patch("clickhouse_connect.dbapi.connect")
    def test_datetime_query(self, mock_connect: Mock):
        query = "SELECT name, created_date, updated_datetime FROM table"  # noqa
        expected_data = [
            [
                "name_1",
                datetime.date.fromisoformat("2023-11-01"),
                datetime.datetime.fromisoformat("2023-11-01T10:59:00"),
            ],
        ]
        expected_description = [
            ["name", "String", None, None, None, None, None],
            ["created_date", "Date", None, None, None, None, None],
            ["updated_datetime", "DateTime", None, None, None, None, None],
        ]
        self._test_run_query(mock_connect, query, expected_data, expected_description)

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

        rowcount = len(data)

        if raise_exception:
            self._mock_cursor.execute.side_effect = raise_exception
        self._mock_cursor.fetchall.return_value = data
        self._mock_cursor.description.return_value = description
        self._mock_cursor.rowcount.return_value = rowcount

        response = self._agent.execute_operation(
            "clickhouse",
            "run_query",
            operation_dict,
            {
                "connect_args": _CLICKHOUSE_CREDENTIALS,
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
            **_CLICKHOUSE_CREDENTIALS,
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
        self.assertEqual(rowcount, result["rowcount"])

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
        elif isinstance(value, decimal.Decimal):
            return {
                "__type__": "decimal",
                "__data__": str(value),
            }
        else:
            return value
