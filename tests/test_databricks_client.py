import os
import socket
import sys
from telnetlib import Telnet
from typing import Dict
from unittest import TestCase
from unittest.mock import patch, create_autospec, Mock, call

from requests import Response, HTTPError

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.models import ATTRIBUTE_NAME_ERROR
from apollo.validators.validate_network import _DEFAULT_TIMEOUT_SECS


_DATABRICKS_CREDENTIALS = {
    "server_hostname": "www.test.com",
    "http_path": "/path",
}

_HTTP_USER_AGENT = "TestUserAgent"

_HTTP_OPERATION = {
    "trace_id": "1234",
    "commands": [
        {
            "method": "do_request",
            "kwargs": {
                "url": "https://test.com/path",
                "http_method": "GET",
                "payload": {},
                "user_agent": _HTTP_USER_AGENT,
                "additional_headers": None,
                "retry_status_code_ranges": None,
            },
        }
    ],
}
_HTTP_CREDENTIALS = {"token": "123_token"}


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
        expected_description = [["catalog", "string"]]
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

        self.assertIsNone(response.result.get("__error__"))
        mock_connect.assert_called_with(**_DATABRICKS_CREDENTIALS)
        self._mock_cursor.execute.assert_has_calls(
            [
                call("SET STATEMENT_TIMEOUT = 10;"),
                call("SHOW CATALOGS", None),
            ]
        )
        self.assertTrue("all_results" in response.result)
        self.assertEqual(expected_data, response.result["all_results"])

        self.assertTrue("description" in response.result)
        self.assertEqual(expected_description, response.result["description"])

        self.assertTrue("rowcount" in response.result)
        self.assertEqual(expected_rows, response.result["rowcount"])

    @patch("requests.request")
    def test_http_request(self, mock_request):
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        response = self._agent.execute_operation(
            "http",
            "do_request",
            _HTTP_OPERATION,
            _HTTP_CREDENTIALS,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": f"Bearer {_HTTP_CREDENTIALS['token']}",
                "User-Agent": _HTTP_USER_AGENT,
            },
        )
        mock_response.assert_has_calls(
            [
                call.raise_for_status(),
                call.json(),
            ]
        )
        self.assertEqual(expected_result, response.result)

    @patch("requests.request")
    def test_http_request_failed(self, mock_request):
        mock_response = create_autospec(Response)
        mock_response.status_code = 404
        mock_response.text = "not found"
        mock_request.return_value = mock_response
        mock_response.raise_for_status.side_effect = HTTPError(
            "failed",
            response=mock_response,
        )

        response = self._agent.execute_operation(
            "http",
            "do_request",
            _HTTP_OPERATION,
            _HTTP_CREDENTIALS,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": f"Bearer {_HTTP_CREDENTIALS['token']}",
                "User-Agent": _HTTP_USER_AGENT,
            },
        )
        mock_response.assert_has_calls(
            [
                call.raise_for_status(),
            ]
        )
        self.assertIsNotNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual(mock_response.text, response.result.get(ATTRIBUTE_NAME_ERROR))
