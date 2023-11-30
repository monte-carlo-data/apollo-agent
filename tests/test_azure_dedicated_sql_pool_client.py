import datetime
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
from apollo.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.agent.logging_utils import LoggingUtils

_AZURE_DEDICATED_SQL_POOL_CREDENTIALS = {
    "host": "www.test.com",
    "user": "u",
    "password": "p",
    "port": "1433",
    "db_name": "db",
}


class AzureDedicatedSqlPoolClientTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor
        self.maxDiff = None

    @patch("pyodbc.connect")
    def test_query(self, mock_connect):
        query = "SELECT * FROM (SELECT name, value, Row_number() OVER (ORDER BY name) AS rownum FROM table) AS tableresults WHERE rownum > ? and rownum <= ?"  # noqa
        args = [0, 2]
        expected_data = [
            [
                "name_1",
                11.1,
                1,
            ],
            [
                "name_2",
                22.2,
                2,
            ],
        ]
        expected_description = [
            ["name", "string", None, None, None, None, None],
            ["value", "float", None, None, None, None, None],
            ["rownum", "int", None, None, None, None, None],
        ]
        self._test_run_query(
            mock_connect, query, args, expected_data, expected_description
        )

    @patch("pyodbc.connect")
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
            "azure-dedicated-sql-pool",
            "run_query",
            operation_dict,
            {
                "connect_args": _AZURE_DEDICATED_SQL_POOL_CREDENTIALS,
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

        mock_connect.assert_called_with(**_AZURE_DEDICATED_SQL_POOL_CREDENTIALS)
        self._mock_cursor.execute.assert_has_calls(
            [
                call(query, query_args if query_args else None),
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
