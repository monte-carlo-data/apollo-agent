from unittest import TestCase
from unittest.mock import patch, MagicMock

from apollo.integrations.db.salesforce_data_cloud_proxy_client import (
    SalesforceDataCloudProxyClient,
    SalesforceDataCloudCredentials,
)


class SalesforceDataCloudProxyClientTests(TestCase):
    def setUp(self):
        self.credentials = SalesforceDataCloudCredentials(
            host="test.salesforce.com",
            client_id="test_client_id",
            client_secret="test_client_secret",
            core_token="test_core_token",
            refresh_token="test_refresh_token",
        )

    @patch(
        "apollo.integrations.db.salesforce_data_cloud_proxy_client.SalesforceCDPConnection"
    )
    def test_init(self, mock_connection):
        salesforce_cdp_mock = mock_connection.return_value

        client = SalesforceDataCloudProxyClient(credentials=self.credentials)

        # Verify SalesforceCDPConnection was called with correct arguments
        mock_connection.assert_called_once_with(
            f"https://{self.credentials.host}",
            client_id=self.credentials.client_id,
            client_secret=self.credentials.client_secret,
            core_token=self.credentials.core_token,
            refresh_token=self.credentials.refresh_token,
        )
        # Verify connection is set
        self.assertEqual(client._connection, salesforce_cdp_mock)
        # Verify connection type
        self.assertEqual(client._connection_type, "salesforce-data-cloud")

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

        client = SalesforceDataCloudProxyClient(credentials=self.credentials)
        result = client.list_tables()

        # Verify list_tables was called
        salesforce_cdp_mock.list_tables.assert_called_once()

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
        self.assertEqual(result, expected_result)
