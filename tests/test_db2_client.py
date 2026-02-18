from unittest import TestCase
from unittest.mock import Mock, patch

import ibm_db_dbi

from apollo.agent.agent import Agent
from apollo.common.agent.constants import ATTRIBUTE_NAME_RESULT
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.db.db2_proxy_client import Db2ProxyClient

STRING_TYPE = ibm_db_dbi.DBAPITypeObject(("CHARACTER", "CHAR", "VARCHAR"))
NUMBER_TYPE = ibm_db_dbi.DBAPITypeObject(("DECIMAL",))

_DB2_CREDENTIALS = {
    "connect_args": {
        "DATABASE": "testdb",
        "HOSTNAME": "localhost",
        "PORT": "50000",
        "PROTOCOL": "TCPIP",
        "UID": "testuser",
        "PWD": "testpass",
    }
}


class Db2ClientTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor
        self.maxDiff = None

    @patch("ibm_db_dbi.Connection")
    @patch("ibm_db.connect")
    def test_run_query(self, mock_ibm_db_connect, mock_dbi_connection):
        mock_ibm_db_connection = Mock()
        mock_ibm_db_connect.return_value = mock_ibm_db_connection
        mock_dbi_connection.return_value = self._mock_connection

        query = "SELECT name, value FROM table"
        data = [["name_1", 11.1], ["name_2", 22.2]]
        # Use actual DBAPITypeObject instances to simulate real DB2 cursor behavior
        description = [
            ["name", STRING_TYPE, None, None, None, None, None],
            ["value", NUMBER_TYPE, None, None, None, None, None],
        ]

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

        mock_ibm_db_connect.return_value = Mock()
        mock_dbi_connection.return_value = self._mock_connection

        self._mock_cursor.fetchall.return_value = data
        self._mock_cursor.description = description
        self._mock_cursor.rowcount = len(data)

        result = self._agent.execute_operation(
            connection_type="db2",
            operation_name="test_operation",
            operation_dict=operation_dict,
            credentials=_DB2_CREDENTIALS,
        )

        self.assertIn(ATTRIBUTE_NAME_RESULT, result.result)
        # The description should be processed to convert DBAPITypeObject instances to strings
        serialized_description = [
            ["name", "CHARACTER", None, None, None, None, None],
            ["value", "DECIMAL", None, None, None, None, None],
        ]
        expected_result = {
            "all_results": data if data is not None else data,
            "description": serialized_description,
            "rowcount": len(data),
        }
        self.assertEqual(result.result[ATTRIBUTE_NAME_RESULT], expected_result)

        expected_connection_string = "DATABASE=testdb;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=testuser;PWD=testpass"
        mock_ibm_db_connect.assert_called_once_with(expected_connection_string, "", "")

    @patch("ibm_db_dbi.Connection")
    @patch("ibm_db.connect")
    def test_run_query_empty_result(self, mock_ibm_db_connect, mock_dbi_connection):
        """When the query returns no rows, DB2 cursor can return None from fetchall/description."""
        mock_ibm_db_connection = Mock()
        mock_ibm_db_connect.return_value = mock_ibm_db_connection
        mock_dbi_connection.return_value = self._mock_connection

        query = "SELECT name FROM table WHERE 1=0"
        # DB2 (ibm_db_dbi) can return None for fetchall() and description when no rows
        self._mock_cursor.fetchall.return_value = None
        self._mock_cursor.description = None
        self._mock_cursor.rowcount = 0

        operation_dict = {
            "trace_id": "1234",
            "skip_cache": True,
            "commands": [
                {"method": "cursor", "store": "_cursor"},
                {
                    "target": "_cursor",
                    "method": "execute",
                    "args": [query, None],
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

        result = self._agent.execute_operation(
            connection_type="db2",
            operation_name="test_operation",
            operation_dict=operation_dict,
            credentials=_DB2_CREDENTIALS,
        )

        self.assertIn(ATTRIBUTE_NAME_RESULT, result.result)
        self.assertIsNone(result.result.get("error"))
        expected_result = {
            "all_results": [],
            "description": [],
            "rowcount": 0,
        }
        self.assertEqual(result.result[ATTRIBUTE_NAME_RESULT], expected_result)

    def test_invalid_credentials(self):
        with self.assertRaises(ValueError) as context:
            Db2ProxyClient(credentials={})
        self.assertIn("DB2 agent client requires connect_args", str(context.exception))

    @patch("ibm_db_dbi.Connection")
    @patch("ibm_db.connect")
    @patch("os.path.exists")
    @patch("os.unlink")
    def test_connect_with_ssl(
        self, mock_unlink, mock_exists, mock_ibm_db_connect, mock_dbi_connection
    ):
        mock_ibm_db_connection = Mock()
        mock_ibm_db_connect.return_value = mock_ibm_db_connection
        mock_dbi_connection.return_value = self._mock_connection
        mock_exists.return_value = True

        ca_cert_data = (
            "-----BEGIN CERTIFICATE-----\nCA_CERT_DATA\n-----END CERTIFICATE-----"
        )
        credentials = {
            "connect_args": {
                "DATABASE": "testdb",
                "HOSTNAME": "localhost",
                "PORT": "50001",  # SSL port
                "PROTOCOL": "TCPIP",
                "UID": "testuser",
                "PWD": "testpass",
            },
            "ssl_options": {
                "ca_data": ca_cert_data,
                "disabled": False,
            },
        }

        client = Db2ProxyClient(credentials)

        # Verify ibm_db.connect was called with SSL parameters
        mock_ibm_db_connect.assert_called_once()
        connection_string = mock_ibm_db_connect.call_args[0][0]

        # Verify SSL parameters are in connection string
        self.assertIn("Security=SSL", connection_string)
        self.assertIn("SSLServerCertificate=", connection_string)
        self.assertIn("PORT=50001", connection_string)
