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

    # Constructor/Initialization Tests
    def test_init_missing_credentials(self):
        with self.assertRaises(ValueError) as context:
            SalesforceCRMProxyClient(credentials=None)
        self.assertIn("connect_args", str(context.exception))

    def test_init_empty_credentials(self):
        with self.assertRaises(ValueError) as context:
            SalesforceCRMProxyClient(credentials={})
        self.assertIn("connect_args", str(context.exception))

    def test_init_missing_connect_args(self):
        with self.assertRaises(ValueError) as context:
            SalesforceCRMProxyClient(credentials={"other_key": "value"})
        self.assertIn("connect_args", str(context.exception))

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_init_successful(self, mock_salesforce: MagicMock):
        mock_sf_instance = MagicMock()
        mock_salesforce.return_value = mock_sf_instance

        client = SalesforceCRMProxyClient(credentials=self.credentials)

        # Verify Salesforce was called with correct arguments
        mock_salesforce.assert_called_once_with(**self.credentials["connect_args"])
        # Verify connection is set
        self.assertEqual(client._connection, mock_sf_instance)
        # Verify connection type
        self.assertEqual(client._connection_type, "salesforce-crm")

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_wrapped_client_property(self, mock_salesforce: MagicMock):
        mock_sf_instance = MagicMock()
        mock_salesforce.return_value = mock_sf_instance

        client = SalesforceCRMProxyClient(credentials=self.credentials)

        self.assertEqual(client.wrapped_client, mock_sf_instance)

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_close_method(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)

        # Should not raise any exception
        client.close()

    # Filter attributes tests
    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_infer_cursor_description_filters_attributes(
        self, mock_salesforce: MagicMock
    ):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)

        test_row = {
            "attributes": {"type": "Account", "url": "/test"},
            "Id": "001ABC123",
            "Name": "Test Account",
        }

        description = client._infer_cursor_description(test_row)

        # Should not include attributes field
        field_names = [desc[0] for desc in description]
        self.assertNotIn("attributes", field_names)
        self.assertIn("Id", field_names)
        self.assertIn("Name", field_names)

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_execute_filters_attributes_from_records(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)

        client._connection.query_all.return_value = {
            "totalSize": 1,
            "records": [
                {
                    "attributes": {"type": "Account", "url": "/test"},
                    "Id": "001ABC123",
                    "Name": "Test Account",
                }
            ],
        }

        result = client.execute("SELECT Id, Name FROM Account")

        # Should only have Id and Name values, not attributes
        expected_records = [["001ABC123", "Test Account"]]
        self.assertEqual(result["records"], expected_records)

    # Describe Methods Tests
    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_describe_global(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)

        mock_sobjects = [
            {"name": "Account", "label": "Account", "custom": False},
            {"name": "Contact", "label": "Contact", "custom": False},
            {"name": "CustomObject__c", "label": "Custom Object", "custom": True},
        ]
        client._connection.describe.return_value = {"sobjects": mock_sobjects}

        result = client.describe_global()

        self.assertEqual(result["objects"], mock_sobjects)
        client._connection.describe.assert_called_once()

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_describe_global_empty_results(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)

        client._connection.describe.return_value = None

        result = client.describe_global()

        self.assertEqual(result["objects"], [])
        client._connection.describe.assert_called_once()

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_describe_object(self, mock_salesforce: MagicMock):
        mock_connection = MagicMock()
        mock_salesforce.return_value = mock_connection
        client = SalesforceCRMProxyClient(credentials=self.credentials)

        mock_object_description = {
            "name": "Account",
            "label": "Account",
            "fields": [
                {"name": "Id", "type": "id", "label": "Account ID"},
                {"name": "Name", "type": "string", "label": "Account Name"},
            ],
        }

        # Mock the salesforce object by setting it as an attribute on the connection
        mock_sf_object = MagicMock()
        mock_sf_object.describe.return_value = mock_object_description

        # Set the Account attribute directly on the mock connection
        setattr(mock_connection, "Account", mock_sf_object)

        result = client.describe_object("Account")

        self.assertEqual(result["object_description"], mock_object_description)
        mock_sf_object.describe.assert_called_once()

    # Execute Method Tests
    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_execute(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)
        client._connection.query_all.return_value = {
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

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_execute_empty_results(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)
        client._connection.query_all.return_value = {"totalSize": 0, "records": []}

        result = client.execute("SELECT Id FROM Account WHERE 1=0")

        self.assertEqual(result["rowcount"], 0)
        self.assertEqual(result["records"], [])
        self.assertEqual(result["description"], [])

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_execute_with_null_values(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)
        client._connection.query_all.return_value = {
            "totalSize": 2,
            "records": [
                {
                    "attributes": {
                        "type": "Account",
                        "url": "/services/data/v59.0/sobjects/Account/001",
                    },
                    "Id": "001ABC123",
                    "Name": "Test Account",
                    "Phone": None,
                    "Website": None,
                    "AnnualRevenue": None,
                },
                {
                    "attributes": {
                        "type": "Account",
                        "url": "/services/data/v59.0/sobjects/Account/002",
                    },
                    "Id": "002DEF456",
                    "Name": None,
                    "Phone": "(555) 123-4567",
                    "Website": "https://example.com",
                    "AnnualRevenue": 1000000.0,
                },
            ],
        }

        result = client.execute(
            "SELECT Id, Name, Phone, Website, AnnualRevenue FROM Account"
        )

        expected_records = [
            ["001ABC123", "Test Account", None, None, None],
            ["002DEF456", None, "(555) 123-4567", "https://example.com", 1000000.0],
        ]
        self.assertEqual(result["records"], expected_records)
        self.assertEqual(result["rowcount"], 2)

        # Check that description handles null values correctly
        expected_description = [
            ("Id", "str", None, None, None, None, None),
            ("Name", "str", None, None, None, None, None),
            ("Phone", "str", None, None, None, None, None),
            ("Website", "str", None, None, None, None, None),
            (
                "AnnualRevenue",
                "str",
                None,
                None,
                None,
                None,
                None,
            ),  # TODO: None in first row infers to str
        ]
        self.assertEqual(result["description"], expected_description)

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_execute_different_data_types(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)

        # Test with different data types
        test_datetime = "2025-01-15T10:30:45.000+0000"
        test_decimal = 12345.67
        test_boolean = True
        test_integer = 42

        client._connection.query_all.return_value = {
            "totalSize": 1,
            "records": [
                {
                    "attributes": {
                        "type": "Opportunity",
                        "url": "/services/data/v59.0/sobjects/Opportunity/006",
                    },
                    "Id": "006XYZ789",
                    "Name": "Big Deal",
                    "Amount": test_decimal,
                    "IsClosed": test_boolean,
                    "CreatedDate": test_datetime,
                    "NumberOfEmployees": test_integer,
                }
            ],
        }

        result = client.execute(
            "SELECT Id, Name, Amount, IsClosed, CreatedDate, NumberOfEmployees FROM Opportunity"
        )

        expected_records = [
            [
                "006XYZ789",
                "Big Deal",
                test_decimal,
                test_boolean,
                test_datetime,
                test_integer,
            ]
        ]
        self.assertEqual(result["records"], expected_records)
        self.assertEqual(result["rowcount"], 1)

        # Check type inference for different data types
        expected_description = [
            ("Id", "str", None, None, None, None, None),
            ("Name", "str", None, None, None, None, None),
            ("Amount", "float", None, None, None, None, None),
            ("IsClosed", "bool", None, None, None, None, None),
            ("CreatedDate", "str", None, None, None, None, None),
            ("NumberOfEmployees", "int", None, None, None, None, None),
        ]
        self.assertEqual(result["description"], expected_description)

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_execute_large_result_set(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)

        # Create multiple records
        records = []
        for i in range(5000):
            records.append(
                {
                    "attributes": {
                        "type": "Contact",
                        "url": f"/services/data/v59.0/sobjects/Contact/00{i}",
                    },
                    "Id": f"00{i}ABC{i}{i}{i}",
                    "FirstName": f"First{i}",
                    "LastName": f"Last{i}",
                    "Email": f"test{i}@example.com",
                }
            )

        client._connection.query_all.return_value = {
            "totalSize": 5000,
            "records": records,
        }

        result = client.execute(
            "SELECT Id, FirstName, LastName, Email FROM Contact LIMIT 5"
        )

        expected_records = [
            [f"00{i}ABC{i}{i}{i}", f"First{i}", f"Last{i}", f"test{i}@example.com"]
            for i in range(5000)
        ]
        self.assertEqual(result["records"], expected_records)
        self.assertEqual(result["rowcount"], 5000)
        self.assertEqual(len(result["description"]), 4)  # 4 fields

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_execute_realistic_account_query(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)

        client._connection.query_all.return_value = {
            "totalSize": 2,
            "records": [
                {
                    "attributes": {
                        "type": "Account",
                        "url": "/services/data/v59.0/sobjects/Account/001",
                    },
                    "Id": "001ABC123DEF456",
                    "Name": "Acme Corporation",
                    "Type": "Customer - Direct",
                    "Industry": "Technology",
                    "AnnualRevenue": 5000000.00,
                    "NumberOfEmployees": 250,
                    "BillingCity": "San Francisco",
                    "BillingState": "CA",
                    "CreatedDate": "2024-01-15T08:30:00.000+0000",
                },
                {
                    "attributes": {
                        "type": "Account",
                        "url": "/services/data/v59.0/sobjects/Account/002",
                    },
                    "Id": "002GHI789JKL012",
                    "Name": "Global Industries Inc",
                    "Type": "Prospect",
                    "Industry": "Manufacturing",
                    "AnnualRevenue": 12000000.00,
                    "NumberOfEmployees": 500,
                    "BillingCity": "New York",
                    "BillingState": "NY",
                    "CreatedDate": "2024-02-20T14:45:30.000+0000",
                },
            ],
        }

        query = """
        SELECT Id, Name, Type, Industry, AnnualRevenue, NumberOfEmployees,
               BillingCity, BillingState, CreatedDate
        FROM Account
        WHERE Industry IN ('Technology', 'Manufacturing')
        ORDER BY CreatedDate DESC
        LIMIT 10
        """

        result = client.execute(query)

        self.assertEqual(result["rowcount"], 2)
        self.assertEqual(len(result["records"]), 2)
        self.assertEqual(len(result["description"]), 9)  # 9 fields selected

        # Verify first record
        first_record = result["records"][0]
        self.assertEqual(first_record[0], "001ABC123DEF456")  # Id
        self.assertEqual(first_record[1], "Acme Corporation")  # Name
        self.assertEqual(first_record[2], "Customer - Direct")  # Type

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_execute_contact_with_relationships(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)

        client._connection.query_all.return_value = {
            "totalSize": 1,
            "records": [
                {
                    "attributes": {
                        "type": "Contact",
                        "url": "/services/data/v59.0/sobjects/Contact/003",
                    },
                    "Id": "003MNO345PQR678",
                    "FirstName": "John",
                    "LastName": "Doe",
                    "Email": "john.doe@example.com",
                    "AccountId": "001ABC123DEF456",
                    "Account": {
                        "attributes": {
                            "type": "Account",
                            "url": "/services/data/v59.0/sobjects/Account/001",
                        },
                        "Name": "Acme Corporation",
                    },
                }
            ],
        }

        result = client.execute(
            "SELECT Id, FirstName, LastName, Email, AccountId, Account.Name FROM Contact"
        )
        account_name_dict = {
            "attributes": {
                "type": "Account",
                "url": "/services/data/v59.0/sobjects/Account/001",
            },
            "Name": "Acme Corporation",
        }

        self.assertEqual(result["rowcount"], 1)
        record = result["records"][0]

        self.assertEqual(record[0], "003MNO345PQR678")  # Id
        self.assertEqual(record[1], "John")  # FirstName
        self.assertEqual(record[2], "Doe")  # LastName
        self.assertEqual(record[3], "john.doe@example.com")  # Email
        self.assertEqual(record[4], "001ABC123DEF456")  # AccountId
        self.assertEqual(record[5], account_name_dict)  # TODO: Handle nested dict

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_execute_with_complex_nested_data(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)

        client._connection.query_all.return_value = {
            "totalSize": 1,
            "records": [
                {
                    "attributes": {"type": "Opportunity", "url": "/test"},
                    "Id": "006ABC123",
                    "Name": "Big Deal",
                    "Account": {
                        "attributes": {"type": "Account", "url": "/test"},
                        "Id": "001DEF456",
                        "Name": "Customer Corp",
                    },
                    "OpportunityLineItems": {
                        "totalSize": 2,
                        "records": [
                            {"Id": "00k111", "Quantity": 10},
                            {"Id": "00k222", "Quantity": 5},
                        ],
                    },
                }
            ],
        }

        result = client.execute(
            "SELECT Id, Name, Account.Name, (SELECT Id, Quantity FROM OpportunityLineItems) FROM Opportunity"
        )

        self.assertEqual(result["rowcount"], 1)
        record = result["records"][0]
        self.assertEqual(record[0], "006ABC123")  # Id
        self.assertEqual(record[1], "Big Deal")  # Name
        # Complex nested objects should be preserved as-is
        # TODO: Handle nested dicts
        self.assertIsInstance(record[2], dict)  # Account relationship
        self.assertIsInstance(record[3], dict)  # OpportunityLineItems subquery

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_execute_count_query(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)

        client._connection.query.return_value = {
            "done": True,
            "totalSize": 100,
            "records": ["not relevant to test"],
        }

        result = client.execute_count_query("SELECT Id FROM Account")

        self.assertEqual(result["rowcount"], 1)
        self.assertEqual(result["records"], [[100]])
        self.assertEqual(
            result["description"], [("ROW_COUNT", "int", None, None, None, None, None)]
        )

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_execute_row_limit(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)

        client._connection.query_all_iter.return_value = iter(
            [
                {
                    "attributes": {"type": "Account", "url": "/test"},
                    "Id": "001",
                    "Name": "Account 1",
                },
                {
                    "attributes": {"type": "Account", "url": "/test"},
                    "Id": "002",
                    "Name": "Account 2",
                },
                {
                    "attributes": {"type": "Account", "url": "/test"},
                    "Id": "003",
                    "Name": "Account 3",
                },
                {
                    "attributes": {"type": "Account", "url": "/test"},
                    "Id": "004",
                    "Name": "Account 4",
                },
                {
                    "attributes": {"type": "Account", "url": "/test"},
                    "Id": "005",
                    "Name": "Account 5",
                },
            ]
        )

        result = client.execute_row_limit("SELECT Id, Name FROM Account", 3)

        self.assertEqual(result["rowcount"], 3)
        self.assertEqual(
            result["records"],
            [["001", "Account 1"], ["002", "Account 2"], ["003", "Account 3"]],
        )
        self.assertEqual(
            result["description"],
            [
                ("Id", "str", None, None, None, None, None),
                ("Name", "str", None, None, None, None, None),
            ],
        )

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_execute_row_limit_negative_limit(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)

        client._connection.query_all_iter.return_value = iter(
            [{"totalSize": 0, "records": []}]
        )

        result = client.execute_row_limit("SELECT Id, Name FROM Account", -1)

        self.assertEqual(result["rowcount"], 0)
        self.assertEqual(result["records"], [])
        self.assertEqual(result["description"], [])

    @patch("apollo.integrations.db.salesforce_crm_proxy_client.Salesforce")
    def test_execute_row_limit_zero_limit(self, mock_salesforce: MagicMock):
        mock_salesforce.return_value = MagicMock()
        client = SalesforceCRMProxyClient(credentials=self.credentials)

        client._connection.query_all_iter.return_value = iter(
            [{"totalSize": 0, "records": []}]
        )

        result = client.execute_row_limit("SELECT Id, Name FROM Account", 0)

        self.assertEqual(result["rowcount"], 0)
        self.assertEqual(result["records"], [])
        self.assertEqual(result["description"], [])
