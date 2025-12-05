import json
import uuid
from unittest import TestCase
from unittest.mock import Mock

import responses
from salesforcecdpconnector.exceptions import Error

from apollo.agent.agent import Agent
from apollo.agent.constants import (
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_STACK_TRACE,
)
from apollo.agent.logging_utils import LoggingUtils


class SalesforceDataCloudProxyClientTests(TestCase):
    def setUp(self):
        self.agent = Agent(LoggingUtils())
        self.credentials = {
            "connect_args": {
                "domain": "test.salesforce.com",
                "client_id": "test_client_id",
                "client_secret": "test_client_secret",
                "core_token": "test_core_token",  # Default is client credentials which only has core_token
            }
        }

        self.mock_responses = responses.RequestsMock()
        self.mock_responses.start()
        self.mock_responses.reset()

        self.addCleanup(self.mock_responses.stop)
        self.addCleanup(self.mock_responses.reset)

        self.setup_salesforce_data_cloud_api()

    def setup_salesforce_data_cloud_api(self):
        self.metadata_response = [
            {
                "name": "Account",
                "displayName": "Account",
                "fields": [
                    {"name": "Id", "displayName": "Id", "type": "STRING"},
                    {"name": "Name", "displayName": "Name", "type": "STRING"},
                    {
                        "name": "CreatedDate",
                        "displayName": "Created Date",
                        "type": "DATE_TIME",
                    },
                ],
            },
            {
                "name": "Contact",
                "displayName": "Contact",
                "fields": [
                    {"name": "Id", "displayName": "Id", "type": "STRING"},
                    {
                        "name": "FirstName",
                        "displayName": "First Name",
                        "type": "STRING",
                    },
                    {"name": "LastName", "displayName": "Last Name", "type": "STRING"},
                    {"name": "Email", "displayName": "Email", "type": "STRING"},
                ],
            },
            {
                "name": "Opportunity",
                "displayName": "Opportunity",
                "fields": [
                    {"name": "Id", "displayName": "Id", "type": "STRING"},
                    {"name": "Amount", "displayName": "Amount", "type": "DECIMAL"},
                    {
                        "name": "CloseDate",
                        "displayName": "Close Date",
                        "type": "DATE_TIME",
                    },
                ],
            },
        ]

        self.data_response = {
            "data": [
                ["Account1", "Active", "2021-09-16T16:26:36+00:00"],
                ["Account2", "Inactive", "2023-01-02T14:20:00+00:00"],
            ],
            "startTime": "2022-03-07T19:57:19.374525Z",
            "endTime": "2022-03-07T19:57:20.063372Z",
            "rowCount": 3,
            "queryId": "20220307_195719_00109_5frjj",
            "nextBatchId": "fa489494-ff42-45ce-afd6-b838854b5a99",
            "done": True,
            "metadata": {
                "Name": {
                    "type": "VARCHAR",
                    "placeInOrder": 0,
                },
                "Status": {
                    "type": "VARCHAR",
                    "placeInOrder": 1,
                },
                "CreatedDate": {
                    "type": "TIMESTAMP",
                    "placeInOrder": 2,
                },
            },
        }

        self.client_credentials_token = str(uuid.uuid4())
        self.api_token = str(uuid.uuid4())

        self.client_credentials_token_endpoint = Mock(
            return_value=(
                200,
                {},
                json.dumps({"access_token": self.client_credentials_token}),
            )
        )
        self.mock_responses.add_callback(
            method=responses.POST,
            url="https://test.salesforce.com/services/oauth2/token",
            callback=self.client_credentials_token_endpoint,
        )

        self.api_token_endpoint = Mock(
            return_value=(
                200,
                {},
                json.dumps(
                    {
                        "access_token": self.api_token,
                        "expires_in": 3600,
                        "instance_url": "test.salesforce.com",
                    }
                ),
            )
        )
        self.mock_responses.add_callback(
            method=responses.POST,
            url="https://test.salesforce.com/services/a360/token",
            callback=self.api_token_endpoint,
        )

        self.metadata_endpoint = Mock(
            return_value=(200, {}, json.dumps({"metadata": self.metadata_response}))
        )
        self.mock_responses.add_callback(
            method=responses.GET,
            url="https://test.salesforce.com/api/v1/metadata",
            callback=self.metadata_endpoint,
        )

        self.query_endpoint = Mock(
            return_value=(200, {}, json.dumps(self.data_response))
        )
        self.mock_responses.add_callback(
            method=responses.POST,
            url="https://test.salesforce.com/api/v2/query",
            callback=self.query_endpoint,
        )

    def test_init(self):
        # Test that the agent can create a SalesforceDataCloudProxyClient via execute_operation
        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,  # Force a new client to be created
            "commands": [{"method": "_connection_type"}],
        }

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_init",
            operation_dict=operation,
            credentials=self.credentials,
        )

        # Verify the operation was successful and returned the connection type
        self.assertFalse(response.is_error)
        self.assertEqual(
            response.result[ATTRIBUTE_NAME_RESULT], "salesforce-data-cloud"
        )

    def test_init_with_refresh_token(self):
        # Test that the agent can create a SalesforceDataCloudProxyClient via execute_operation
        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,  # Force a new client to be created
            "commands": [{"method": "_connection_type"}],
        }

        del self.credentials["connect_args"][
            "core_token"
        ]  # Using refresh_token instead of core_token
        self.credentials["connect_args"]["refresh_token"] = "required_but_not_used"

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_init",
            operation_dict=operation,
            credentials=self.credentials,
        )

        # Verify the operation was successful and returned the connection type
        self.assertFalse(response.is_error)
        self.assertEqual(
            response.result[ATTRIBUTE_NAME_RESULT], "salesforce-data-cloud"
        )

    def test_list_tables(self):
        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,  # Force a new client to be created
            "commands": [{"method": "list_tables"}],
        }

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_list_tables",
            operation_dict=operation,
            credentials=self.credentials,
        )

        tables = response.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual(len(tables), len(self.metadata_response))

        for mock_table in self.metadata_response:
            table = next(t for t in tables if t.get("name") == mock_table["name"])
            self.assertEqual(len(table["fields"]), len(mock_table["fields"]))
            for mock_field in mock_table["fields"]:
                field = next(
                    f for f in table["fields"] if f.get("name") == mock_field["name"]
                )
                self.assertEqual(field.get("type"), mock_field["type"])

        # Verify that the metadata was cached and not re-fetched for fetch_columns
        self.metadata_endpoint.assert_called_once()

    def test_sql_query_execution(self):
        sql_query = "SELECT Name, Status, CreatedDate FROM Account LIMIT 10"
        commands = [
            {"method": "cursor", "store": "_cursor"},
            {"args": [sql_query], "method": "execute", "target": "_cursor"},
            {"method": "fetchall", "store": "tmp_1", "target": "_cursor"},
            {"method": "description", "store": "tmp_2", "target": "_cursor"},
            {"method": "close", "target": "_cursor"},
            {
                "kwargs": {
                    "all_results": {"__reference__": "tmp_1"},
                    "description": {"__reference__": "tmp_2"},
                },
                "method": "build_dict",
                "target": "__utils",
            },
        ]
        operation = {
            "commands": commands,
            "skip_cache": True,
            "trace_id": "f6e0e3fe-e03c-4f6f-9bfd-55478350ea45",
        }

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_list_tables",
            operation_dict=operation,
            credentials=self.credentials,
        )

        self.assertFalse(response.is_error)
        result = response.result[ATTRIBUTE_NAME_RESULT]

        for i, row in enumerate(self.data_response["data"]):
            self.assertEqual(result["all_results"][i][0], row[0])
            self.assertEqual(result["all_results"][i][1], row[1])

        for i, (key, value) in enumerate(self.data_response["metadata"].items()):
            self.assertEqual(result["description"][i][0], key)
            self.assertEqual(result["description"][i][1], value["type"])

    def test_monkey_patch_handles_non_json_error_response(self):
        self.metadata_endpoint.return_value = (
            500,
            {"content-type": "text/html"},
            "<html><body><h1>500 Internal Server Error</h1></body></html>",
        )

        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,  # Force a new client to be created
            "commands": [{"method": "list_tables"}],
        }

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_monkey_patch",
            operation_dict=operation,
            credentials=self.credentials,
        )

        # Verify the operation returned an error
        self.assertTrue(response.is_error)

        # Verify it's the patched error message
        error_message = response.result[ATTRIBUTE_NAME_ERROR]
        self.assertIn("could not be parsed", error_message)
        self.assertIn("Failed executing metadata query on server", error_message)
