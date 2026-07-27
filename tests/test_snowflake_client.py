import base64
import datetime
import json
from typing import List, Any, Optional, Dict
from unittest import TestCase
from unittest.mock import Mock, call, patch
from snowflake.connector.errors import ProgrammingError

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
    ATTRIBUTE_NAME_ERROR_ATTRS,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.proxy_client_factory import ProxyClientFactory

_SF_CREDENTIALS = {"user": "u", "password": "p", "account": "a", "warehouse": "w"}
# Expected connect() kwargs after CTP applies connect_args_defaults (application injected).
_SF_EXPECTED_CONNECT_ARGS = {**_SF_CREDENTIALS, "application": "Monte Carlo"}


class SnowflakeClientTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor

    @patch("snowflake.connector.connect")
    def test_private_key_auth(self, mock_connect):
        mock_connect.return_value = self._mock_connection

        private_key = b"abc"
        # Credentials arrive from the wire in encoded form; the factory decodes
        # them via decode_dictionary before creating the proxy client.
        credentials = {
            "connect_args": {
                "user": "u",
                "private_key": {
                    "__type__": "bytes",
                    "__data__": base64.b64encode(private_key).decode("utf-8"),
                },
                "account": "a",
                "warehouse": "w",
            },
        }
        client = ProxyClientFactory.get_proxy_client(
            "snowflake", credentials, True, "AWS"
        )
        self.assertIsNotNone(client)
        mock_connect.assert_called_once_with(
            application="Monte Carlo",
            user="u",
            account="a",
            warehouse="w",
            private_key=private_key,
        )

    @patch("snowflake.connector.connect")
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

    @patch("snowflake.connector.connect")
    def test_query_bytearray(self, mock_connect):
        query = "SELECT name, value FROM table"  # noqa
        expected_data = [
            [
                bytearray(b"name_1"),
                11.1,
            ],
            [
                bytearray(b"name_1"),
                22.2,
            ],
        ]
        expected_description = [
            ["name", "binary", None, None, None, None, None],
            ["value", "float", None, None, None, None, None],
        ]
        self._test_run_query(mock_connect, query, expected_data, expected_description)

    @patch("snowflake.connector.connect")
    def test_query_time(self, mock_connect):
        query = "SELECT name, value FROM table"  # noqa
        expected_data = [
            [
                "name_1",
                datetime.time.fromisoformat("14:23:10"),
            ],
            [
                "name_2",
                datetime.time.fromisoformat("14:23:12"),
            ],
        ]
        expected_description = [
            ["name", "string", None, None, None, None, None],
            ["value", "time", None, None, None, None, None],
        ]
        self._test_run_query(mock_connect, query, expected_data, expected_description)

    @patch("snowflake.connector.connect")
    def test_programming_error(self, mock_connect):
        query = ""
        data = []
        description = []
        self._test_run_query(
            mock_connect,
            query,
            data,
            description,
            raise_exception=ProgrammingError("invalid sql", errno=123, sqlstate="abc"),
            expected_error_type="ProgrammingError",
            expected_error_attrs={
                "errno": 123,
                "sqlstate": "abc",
            },
        )

    def _test_run_query(
        self,
        mock_connect: Mock,
        query: str,
        data: List,
        description: List,
        raise_exception: Optional[Exception] = None,
        expected_error_type: Optional[str] = None,
        expected_error_attrs: Optional[Dict] = None,
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
            "snowflake",
            "run_query",
            operation_dict,
            {
                "connect_args": _SF_CREDENTIALS,
            },
        )

        if raise_exception:
            self.assertEqual(
                str(raise_exception), response.result.get(ATTRIBUTE_NAME_ERROR)
            )
            self.assertEqual(
                expected_error_type, response.result.get(ATTRIBUTE_NAME_ERROR_TYPE)
            )
            self.assertEqual(
                expected_error_attrs, response.result.get(ATTRIBUTE_NAME_ERROR_ATTRS)
            )
            return

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        result = response.result.get(ATTRIBUTE_NAME_RESULT)

        mock_connect.assert_called_with(**_SF_EXPECTED_CONNECT_ARGS)
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
        elif isinstance(value, datetime.time):
            return {
                "__type__": "time",
                "__data__": value.isoformat(),
            }
        elif isinstance(value, bytes) or isinstance(value, bytearray):
            return {
                "__type__": "bytes",
                "__data__": base64.b64encode(value).decode("utf-8"),
            }
        else:
            return value

    @patch("apollo.integrations.snowflake.snowflake_proxy_client.requests.request")
    @patch("snowflake.connector.connect")
    def test_rest_request_json_success(self, mock_connect, mock_request):
        mock_connect.return_value = self._mock_connection
        self._mock_connection.rest.token = "tok-123"
        self._mock_connection.rest.server_url = (
            "https://acct.snowflakecomputing.com:443"
        )
        mock_request.return_value = Mock(
            status_code=200, text=json.dumps({"databases": []})
        )

        response = self._agent.execute_operation(
            "snowflake",
            "run_rest_request",
            {
                "trace_id": "t1",
                "skip_cache": True,
                "commands": [
                    {
                        "method": "execute_rest_request",
                        "kwargs": {
                            "method": "GET",
                            "path": "/api/v2/databases",
                            "body": {"messages": []},
                            "timeout": 60,
                        },
                    }
                ],
            },
            {"connect_args": _SF_CREDENTIALS},
        )
        result = response.result.get(ATTRIBUTE_NAME_RESULT)
        self.assertEqual(result, {"status_code": 200, "response": {"databases": []}})
        args, kwargs = mock_request.call_args
        self.assertEqual(args[0], "GET")
        self.assertEqual(
            args[1], "https://acct.snowflakecomputing.com:443/api/v2/databases"
        )
        self.assertEqual(
            kwargs["headers"]["Authorization"], 'Snowflake Token="tok-123"'
        )
        self.assertEqual(kwargs["json"], {"messages": []})
        self.assertEqual(kwargs["timeout"], 60)

    @patch("apollo.integrations.snowflake.snowflake_proxy_client.requests.request")
    @patch("snowflake.connector.connect")
    def test_rest_request_2xx_non_json_body_is_token_redacted(
        self, mock_connect, mock_request
    ):
        mock_connect.return_value = self._mock_connection
        self._mock_connection.rest.token = "tok-123"
        self._mock_connection.rest.server_url = (
            "https://acct.snowflakecomputing.com:443"
        )
        mock_request.return_value = Mock(
            status_code=200,
            text='session Snowflake Token="tok-123" echoed back',
            **{"json.side_effect": ValueError("not json")},
        )

        response = self._agent.execute_operation(
            "snowflake",
            "run_rest_request",
            {
                "trace_id": "t1",
                "skip_cache": True,
                "commands": [
                    {
                        "method": "execute_rest_request",
                        "kwargs": {
                            "method": "GET",
                            "path": "/api/v2/databases",
                            "body": None,
                            "timeout": 60,
                        },
                    }
                ],
            },
            {"connect_args": _SF_CREDENTIALS},
        )
        result = response.result.get(ATTRIBUTE_NAME_RESULT)
        self.assertEqual(result["status_code"], 200)
        self.assertNotIn("tok-123", result["response"])
        self.assertIn("***", result["response"])

    @patch("apollo.integrations.snowflake.snowflake_proxy_client.requests.request")
    @patch("snowflake.connector.connect")
    def test_rest_request_2xx_non_json_body_returns_plain_text(
        self, mock_connect, mock_request
    ):
        mock_connect.return_value = self._mock_connection
        self._mock_connection.rest.token = "tok-123"
        self._mock_connection.rest.server_url = (
            "https://acct.snowflakecomputing.com:443"
        )
        mock_request.return_value = Mock(
            status_code=200,
            text="plain text response",
            **{"json.side_effect": ValueError("not json")},
        )

        response = self._agent.execute_operation(
            "snowflake",
            "run_rest_request",
            {
                "trace_id": "t1",
                "skip_cache": True,
                "commands": [
                    {
                        "method": "execute_rest_request",
                        "kwargs": {
                            "method": "GET",
                            "path": "/api/v2/databases",
                            "body": None,
                            "timeout": 60,
                        },
                    }
                ],
            },
            {"connect_args": _SF_CREDENTIALS},
        )
        result = response.result.get(ATTRIBUTE_NAME_RESULT)
        self.assertEqual(
            result, {"status_code": 200, "response": "plain text response"}
        )

    @patch("apollo.integrations.snowflake.snowflake_proxy_client.requests.request")
    @patch("snowflake.connector.connect")
    def test_rest_request_non_2xx_maps_to_error(self, mock_connect, mock_request):
        mock_connect.return_value = self._mock_connection
        self._mock_connection.rest.token = "tok-123"
        self._mock_connection.rest.server_url = (
            "https://acct.snowflakecomputing.com:443"
        )
        mock_request.return_value = Mock(
            status_code=404,
            text='auth denied for Snowflake Token="tok-123": agent not found',
        )

        response = self._agent.execute_operation(
            "snowflake",
            "run_rest_request",
            {
                "trace_id": "t1",
                "skip_cache": True,
                "commands": [
                    {
                        "method": "execute_rest_request",
                        "kwargs": {"method": "POST", "path": "/api/v2/x", "body": {}},
                    }
                ],
            },
            {"connect_args": _SF_CREDENTIALS},
        )
        result = response.result.get(ATTRIBUTE_NAME_RESULT)
        self.assertEqual(result["status_code"], 404)
        self.assertIn("agent not found", result["error"])
        self.assertNotIn("tok-123", result["error"])
        self.assertIn("***", result["error"])

    @patch("snowflake.connector.connect")
    def test_rest_request_token_none_errors(self, mock_connect):
        mock_connect.return_value = self._mock_connection
        self._mock_connection.rest.token = None
        response = self._agent.execute_operation(
            "snowflake",
            "run_rest_request",
            {
                "trace_id": "t1",
                "skip_cache": True,
                "commands": [
                    {
                        "method": "execute_rest_request",
                        "kwargs": {"method": "GET", "path": "/api/v2/databases"},
                    }
                ],
            },
            {"connect_args": _SF_CREDENTIALS},
        )
        self.assertIsNotNone(response.result.get(ATTRIBUTE_NAME_ERROR))

    @patch("apollo.integrations.snowflake.snowflake_proxy_client.requests.request")
    @patch("snowflake.connector.connect")
    def test_rest_request_rejects_unsafe_path(self, mock_connect, mock_request):
        mock_connect.return_value = self._mock_connection
        self._mock_connection.rest.token = "tok-123"
        self._mock_connection.rest.server_url = (
            "https://acct.snowflakecomputing.com:443"
        )
        for bad in (
            "//evil.example.com/x",
            "//",
            "https://evil.example.com/x",
            "/api\r\nHost: x",
            "api/v2/x",
            "",
        ):
            response = self._agent.execute_operation(
                "snowflake",
                "run_rest_request",
                {
                    "trace_id": "t1",
                    "skip_cache": True,
                    "commands": [
                        {
                            "method": "execute_rest_request",
                            "kwargs": {"method": "GET", "path": bad},
                        }
                    ],
                },
                {"connect_args": _SF_CREDENTIALS},
            )
            self.assertIsNotNone(
                response.result.get(ATTRIBUTE_NAME_ERROR), f"expected error for {bad!r}"
            )
        mock_request.assert_not_called()
