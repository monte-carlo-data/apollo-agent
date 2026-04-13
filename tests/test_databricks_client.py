from datetime import datetime, timezone
from typing import Dict, Any
from unittest import TestCase
from unittest.mock import patch, Mock, call

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.common.agent.constants import ATTRIBUTE_NAME_ERROR, ATTRIBUTE_NAME_RESULT

_DATABRICKS_CREDENTIALS = {
    "server_hostname": "www.test.com",
    "http_path": "/path",
}


class DatabricksClientTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor

    @patch("databricks.sql.connect")
    def test_get_catalogs_temp_vars(self, mock_connect):
        self._test_get_catalogs(
            mock_connect,
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [
                    {"method": "cursor", "store": "_cursor"},
                    {
                        "target": "_cursor",
                        "method": "execute",
                        "args": ["SET STATEMENT_TIMEOUT = 10;"],
                    },
                    {
                        "target": "_cursor",
                        "method": "execute",
                        "args": [
                            "SHOW CATALOGS",
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
            },
        )

    @patch("databricks.sql.connect")
    def test_get_catalogs_calls(self, mock_connect):
        self._test_get_catalogs(
            mock_connect,
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [
                    {"method": "cursor", "store": "_cursor"},
                    {
                        "target": "_cursor",
                        "method": "execute",
                        "args": ["SET STATEMENT_TIMEOUT = 10;"],
                    },
                    {
                        "target": "_cursor",
                        "method": "execute",
                        "args": [
                            "SHOW CATALOGS",
                            None,
                        ],
                    },
                    {
                        "target": "__utils",
                        "method": "build_dict",
                        "kwargs": {
                            "all_results": {
                                "__type__": "call",
                                "target": "_cursor",
                                "method": "fetchall",
                            },
                            "description": {
                                "__type__": "call",
                                "target": "_cursor",
                                "method": "description",
                            },
                            "rowcount": {
                                "__type__": "call",
                                "target": "_cursor",
                                "method": "rowcount",
                            },
                        },
                    },
                ],
            },
        )

    @patch("databricks.sql.connect")
    def test_datetime_values(self, mock_connect: Mock):
        operation_dict = {
            "trace_id": "1234",
            "skip_cache": True,
            "commands": [
                {"method": "cursor", "store": "_cursor"},
                {
                    "target": "_cursor",
                    "method": "execute",
                    "args": [
                        "SELECT CURRENT_TIMESTAMP()",
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
        timestamp = datetime.now(tz=timezone.utc)

        data = [[timestamp]]
        expected_data = [[self._serialized_value(timestamp)]]
        expected_description = [
            ["current_timestamp()", "timestamp", None, None, None, None, None],
        ]
        expected_rows = 1

        self._mock_cursor.fetchall.return_value = data
        self._mock_cursor.description.return_value = expected_description
        self._mock_cursor.rowcount.return_value = expected_rows

        response = self._agent.execute_operation(
            "databricks",
            "current_ts",
            operation_dict,
            {
                "connect_args": _DATABRICKS_CREDENTIALS,
            },
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        result = response.result.get(ATTRIBUTE_NAME_RESULT)
        mock_connect.assert_called_with(**_DATABRICKS_CREDENTIALS)
        self._mock_cursor.execute.assert_has_calls(
            [
                call("SELECT CURRENT_TIMESTAMP()", None),
            ]
        )
        self.assertTrue("all_results" in result)
        self.assertEqual(expected_data, result["all_results"])

        self.assertTrue("description" in result)
        self.assertEqual(expected_description, result["description"])

        self.assertTrue("rowcount" in result)
        self.assertEqual(expected_rows, result["rowcount"])

    def _test_get_catalogs(self, mock_connect: Mock, operation_dict: Dict):
        mock_connect.return_value = self._mock_connection

        expected_data = [
            [
                "catalog_1",
            ],
            [
                "catalog_2",
            ],
        ]
        expected_description = [
            ["catalog", "string", None, None, None, None, None],
        ]
        expected_rows = 2

        self._mock_cursor.fetchall.return_value = expected_data
        self._mock_cursor.description.return_value = expected_description
        self._mock_cursor.rowcount.return_value = expected_rows

        response = self._agent.execute_operation(
            "databricks",
            "get_catalogs",
            operation_dict,
            {
                "connect_args": _DATABRICKS_CREDENTIALS,
            },
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        result = response.result.get(ATTRIBUTE_NAME_RESULT)
        mock_connect.assert_called_with(**_DATABRICKS_CREDENTIALS)
        self._mock_cursor.execute.assert_has_calls(
            [
                call("SET STATEMENT_TIMEOUT = 10;"),
                call("SHOW CATALOGS", None),
            ]
        )
        self.assertTrue("all_results" in result)
        self.assertEqual(expected_data, result["all_results"])

        self.assertTrue("description" in result)
        self.assertEqual(expected_description, result["description"])

        self.assertTrue("rowcount" in result)
        self.assertEqual(expected_rows, result["rowcount"])

    @classmethod
    def _serialized_value(cls, value: Any) -> Any:
        if isinstance(value, datetime):
            return {
                "__type__": "datetime",
                "__data__": value.isoformat(),
            }
        return value
