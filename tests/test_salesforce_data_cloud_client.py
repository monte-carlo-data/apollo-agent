from unittest import TestCase
from unittest.mock import patch, MagicMock

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils


class SalesforceDataCloudProxyClientTests(TestCase):
    def setUp(self):
        self.agent = Agent(LoggingUtils())
        self.credentials = {
            "connect_args": {
                "host": "test.salesforce.com",
                "client_id": "test_client_id",
                "client_secret": "test_client_secret",
                "core_token": "test_core_token",
                "refresh_token": "test_refresh_token",
            }
        }

    @patch(
        "apollo.integrations.db.salesforce_data_cloud_proxy_client.SalesforceCDPConnection"
    )
    def test_init(self, mock_connection):
        salesforce_cdp_mock = mock_connection.return_value

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

        # Verify SalesforceCDPConnection was called with correct arguments
        connect_args = self.credentials["connect_args"]
        mock_connection.assert_called_once_with(
            f"https://{connect_args['host']}",
            client_id=connect_args["client_id"],
            client_secret=connect_args["client_secret"],
            core_token=connect_args["core_token"],
            refresh_token=connect_args["refresh_token"],
        )
        # Verify the operation was successful and returned the connection type
        self.assertFalse(response.is_error)
        self.assertEqual(response.result["__mcd_result__"], "salesforce-data-cloud")

    @patch(
        "apollo.integrations.db.salesforce_data_cloud_proxy_client.SalesforceCDPConnection"
    )
    def test_list_tables(self, mock_connection):
        salesforce_cdp_mock = mock_connection.return_value

        # Create mock tables
        mock_field1 = MagicMock()
        mock_field1.name = "field1"
        mock_field1.display_name = "Field 1"
        mock_field1.type = "string"

        mock_field2 = MagicMock()
        mock_field2.name = "field2"
        mock_field2.display_name = "Field 2"
        mock_field2.type = "number"

        mock_table1 = MagicMock()
        mock_table1.name = "table1"
        mock_table1.display_name = "Table 1"
        mock_table1.category = "category1"
        mock_table1.fields = [mock_field1, mock_field2]

        salesforce_cdp_mock.list_tables.return_value = [mock_table1]

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

        # Verify list_tables was called
        salesforce_cdp_mock.list_tables.assert_called_once()

        # Verify the operation was successful
        self.assertFalse(response.is_error)

        # Verify the result is correctly serialized
        expected_result = [
            {
                "name": "table1",
                "display_name": "Table 1",
                "category": "category1",
                "fields": [
                    {"name": "field1", "displayName": "Field 1", "type": "string"},
                    {"name": "field2", "displayName": "Field 2", "type": "number"},
                ],
            }
        ]
        self.assertEqual(response.result["__mcd_result__"], expected_result)
