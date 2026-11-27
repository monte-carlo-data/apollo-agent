import datetime
import json
from typing import List, Any, Optional
from unittest import TestCase
from unittest.mock import Mock, call, patch

import duckdb
from psycopg2.errors import InsufficientPrivilege  # noqa

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.agent.logging_utils import LoggingUtils

_MOTHERDUCK_CREDENTIALS = "connection_string"


class MotherDuckClientTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor

    @patch("duckdb.connect")
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
            "motherduck",
            "run_query",
            operation_dict,
            {
                "connect_args": _MOTHERDUCK_CREDENTIALS,
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

        mock_connect.assert_called_with(_MOTHERDUCK_CREDENTIALS)
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


class MotherDuckClientSerializationTests(TestCase):
    """Runs queries on a real in-memory duckdb so results carry duckdb's own types."""

    def test_query_result_is_json_serializable(self):
        connection = duckdb.connect(":memory:")
        query = (
            "SELECT 1::INTEGER AS i, DATE '2024-01-02' AS d, "
            "'5f0c3f4e-3b0a-4e1c-9d6a-2a3b4c5d6e7f'::UUID AS u, "
            "INTERVAL 3 DAY AS iv, "
            "['5f0c3f4e-3b0a-4e1c-9d6a-2a3b4c5d6e7f'::UUID] AS ul, "
            "{'u': '5f0c3f4e-3b0a-4e1c-9d6a-2a3b4c5d6e7f'::UUID} AS s, "
            "MAP {DATE '2024-01-01': 1} AS m"
        )
        operation_dict = {
            "trace_id": "1234",
            "skip_cache": True,
            "commands": [
                {"method": "cursor", "store": "_cursor"},
                {"target": "_cursor", "method": "execute", "args": [query, None]},
                {"target": "_cursor", "method": "fetchall", "store": "tmp_1"},
                {"target": "_cursor", "method": "description", "store": "tmp_2"},
                {
                    "target": "__utils",
                    "method": "build_dict",
                    "kwargs": {
                        "all_results": {"__reference__": "tmp_1"},
                        "description": {"__reference__": "tmp_2"},
                    },
                },
            ],
        }

        with patch("duckdb.connect", return_value=connection):
            response = Agent(LoggingUtils()).execute_operation(
                "motherduck",
                "run_query",
                operation_dict,
                {"connect_args": _MOTHERDUCK_CREDENTIALS},
            )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        result = json.loads(response.serialize_result())[ATTRIBUTE_NAME_RESULT]
        self.assertEqual(
            [
                "INTEGER",
                "DATE",
                "UUID",
                "INTERVAL",
                "UUID[]",
                "STRUCT(u UUID)",
                "MAP(DATE, INTEGER)",
            ],
            [col[1] for col in result["description"]],
        )
        uuid_str = "5f0c3f4e-3b0a-4e1c-9d6a-2a3b4c5d6e7f"
        self.assertEqual(
            [
                [
                    1,
                    {"__type__": "date", "__data__": "2024-01-02"},
                    uuid_str,
                    "3 days, 0:00:00",
                    [uuid_str],
                    {"u": uuid_str},
                    {"2024-01-01": 1},
                ]
            ],
            result["all_results"],
        )
