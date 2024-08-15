from unittest import TestCase
from unittest.mock import patch, MagicMock

from apollo.integrations.db.dremio_proxy_client import DremioProxyClient

import pyarrow


class DremioClientTests(TestCase):
    def setUp(self):
        self.credentials = {
            "connect_args": {"host": "localhost", "token": "dummy_token"}
        }
        self.query = "SELECT * FROM test_table"

    @patch("apollo.integrations.db.dremio_proxy_client.flight.connect")
    def test_init_success(self, mock_connect):
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        client = DremioProxyClient(credentials=self.credentials)

        self.assertEqual(client._connection, mock_connection)
        self.assertEqual(client._headers, [(b"authorization", b"bearer dummy_token")])
        mock_connect.assert_called_once_with(location="localhost")

    def test_init_missing_connect_args(self):
        with self.assertRaises(ValueError):
            DremioProxyClient(credentials={})

    @patch("apollo.integrations.db.dremio_proxy_client.flight.connect")
    @patch("apollo.integrations.db.dremio_proxy_client.FlightCallOptions")
    def test_execute(self, mock_flight_call_options, mock_connect):
        mock_connection = MagicMock()
        mock_flight_info = MagicMock()
        mock_flight_call_options_instance = MagicMock()

        mock_flight_call_options.return_value = mock_flight_call_options_instance

        mock_connect.return_value = mock_connection
        mock_connection.get_flight_info.return_value = mock_flight_info
        mock_flight_info.endpoints = [MagicMock(ticket="dummy_ticket")]

        mock_reader = MagicMock()
        mock_reader.read_all.return_value = pyarrow.table(
            {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
        )

        mock_reader.schema = pyarrow.schema(
            [("col1", pyarrow.int32()), ("col2", pyarrow.string())]
        )
        mock_connection.do_get.return_value = mock_reader

        client = DremioProxyClient(credentials=self.credentials)

        result = client.execute(self.query)

        self.assertEqual(result["records"], [[1, "a"], [2, "b"], [3, "c"]])
        self.assertEqual(result["rowcount"], 3)
        self.assertEqual(
            result["description"],
            [
                ["col1", 2, None, None, None, None, None],
                ["col2", 1, None, None, None, None, None],
            ],
        )

    @patch("apollo.integrations.db.dremio_proxy_client.flight.connect")
    def test_set_description(self, mock_connect):
        schema = pyarrow.schema([("col1", pyarrow.int32()), ("col2", pyarrow.string())])

        client = DremioProxyClient(credentials=self.credentials)
        description = client._set_description(schema)

        self.assertEqual(
            description,
            [
                ["col1", 2, None, None, None, None, None],
                ["col2", 1, None, None, None, None, None],
            ],
        )
