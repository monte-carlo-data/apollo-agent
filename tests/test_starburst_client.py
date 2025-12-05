import datetime
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

from apollo.agent.agent import Agent
from apollo.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.agent.logging_utils import LoggingUtils

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
    "port": "443",
    "http_scheme": "https",
    "catalog": "fizz",
    "schema": "buzz",
    "auth": ANY,
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

        self.assertIn("user", response.result.get(ATTRIBUTE_NAME_ERROR))
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
        self.assertIn("password", response.result.get(ATTRIBUTE_NAME_ERROR))
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

        mock_connect.assert_called_with(**_EXPECTED_STARBURST_CREDENTIALS)
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
