from unittest import TestCase
from unittest.mock import patch, MagicMock

from apollo.integrations.db.salesforce_crm_proxy_client import SalesforceCRMProxyClient


class SalesforceCRMProxyClientTests(TestCase):
    def setUp(self):
        self.credentials = {
            "connect_args": {
                "username": "test@example.com",
                "password": "password123",
                "security_token": "token123",
            },
        }

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_execute(self, mock_salesforce):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)
        client._connection.query = MagicMock()
        client._connection.query.return_value = {
            "totalSize": 1,
            "records": [
                {
                    "attributes": {
                        "type": "Account",
                        "url": "/services/data/v59.0/sobjects/Account/001gL0000071FgvQAE",
                    },
                    "Id": "001gL0000071FgvQAE",
                    "IsDeleted": False,
                    "ParentId": None,
                    "AccountNumber": "CD451796",
                    "Name": "Edge Communications",
                    "Phone": "(512) 757-6000",
                    "Website": "http://edgecomm.com",
                    "CreatedDate": "2025-05-22T14:46:46.000+0000",
                    "AnnualRevenue": 139000000.0,
                },
            ],
        }
        query = (
            "SELECT Id, IsDeleted, ParentId, AccountNumber, Name, Phone, Website, CreatedDate, AnnualRevenue "
            "FROM Account "
            "LIMIT 1"
        )
        result = client.execute(query)

        # Check records
        expected_records = [
            [
                "001gL0000071FgvQAE",
                False,
                None,
                "CD451796",
                "Edge Communications",
                "(512) 757-6000",
                "http://edgecomm.com",
                "2025-05-22T14:46:46.000+0000",
                139000000.0,
            ]
        ]
        self.assertEqual(result["records"], expected_records)

        # Check rowcount
        self.assertEqual(result["rowcount"], 1)

        # Check description
        expected_description = [
            ("Id", "str", None, None, None, None, None),
            ("IsDeleted", "bool", None, None, None, None, None),
            ("ParentId", "str", None, None, None, None, None),
            ("AccountNumber", "str", None, None, None, None, None),
            ("Name", "str", None, None, None, None, None),
            ("Phone", "str", None, None, None, None, None),
            ("Website", "str", None, None, None, None, None),
            ("CreatedDate", "str", None, None, None, None, None),
            ("AnnualRevenue", "float", None, None, None, None, None),
        ]
        self.assertEqual(result["description"], expected_description)
