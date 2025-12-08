import datetime
import ssl
from typing import (
    Iterable,
    List,
    Any,
    Optional,
)
from unittest import TestCase
from unittest.mock import Mock, call, patch, MagicMock

from oracledb.base_impl import (
    DB_TYPE_VARCHAR,
    DB_TYPE_NUMBER,
    DbType,
)

from apollo.agent.agent import Agent
from apollo.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.db.oracle_proxy_client import OracleProxyClient

_ORACLE_DB_CREDENTIALS = {
    "dsn": "www.example.com:1521/ORCL",
    "user": "u",
    "password": "p",
}


class OracleDbClientTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor
        self.maxDiff = None

    @patch("oracledb.connect")
    def test_query(self, mock_connect: Mock) -> None:
        query = "SELECT name, value FROM table OFFSET :1 ROWS FETCH NEXT :2 ROWS ONLY"  # noqa
        args = [0, 2]
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
            ["name", DB_TYPE_VARCHAR, None, None, None, None, None],
            ["value", DB_TYPE_NUMBER, None, None, None, None, None],
        ]
        self._test_run_query(
            mock_connect, query, args, expected_data, expected_description
        )

    def _test_run_query(
        self,
        mock_connect: Mock,
        query: str,
        query_args: Optional[Iterable[Any]],
        data: List,
        description: List,
        raise_exception: Optional[Exception] = None,
        expected_error_type: Optional[str] = None,
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
                        query_args,
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
            "oracle",
            "run_query",
            operation_dict,
            {
                "connect_args": _ORACLE_DB_CREDENTIALS,
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

        mock_connect.assert_called_with(expire_time=1, **_ORACLE_DB_CREDENTIALS)
        self._mock_cursor.execute.assert_has_calls(
            [
                call(query, query_args),
            ]
        )
        self._mock_cursor.description.assert_called()
        self._mock_cursor.rowcount.assert_called()

        expected_data = self._serialized_data(data)
        self.assertTrue("all_results" in result)
        self.assertEqual(expected_data, result["all_results"])

        expected_description = self._serialized_description(description)
        self.assertTrue("description" in result)
        self.assertEqual(expected_description, result["description"])

        self.assertTrue("rowcount" in result)
        self.assertEqual(expected_rows, result["rowcount"])

    @classmethod
    def _serialized_data(cls, data: List) -> List:
        return [cls._serialized_row(v) for v in data]

    @classmethod
    def _serialized_description(cls, description: List) -> List:
        return [cls._serialized_col(v) for v in description]

    @classmethod
    def _serialized_row(cls, row: List) -> List:
        return [cls._serialized_value(v) for v in row]

    @classmethod
    def _serialized_col(cls, col: List) -> List:
        return [cls._serialized_value(v) for v in col]

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
        elif isinstance(value, DbType):
            return value.name
        else:
            return value

    @patch("oracledb.connect")
    @patch("apollo.integrations.db.oracle_proxy_client.create_oracle_ssl_context")
    def test_connect_with_ssl_ca_cert(
        self, mock_create_ssl_context: Mock, mock_connect: Mock
    ) -> None:
        """Test Oracle connection with SSL using CA certificate only."""
        mock_ssl_context = MagicMock(spec=ssl.SSLContext)
        mock_create_ssl_context.return_value = mock_ssl_context
        mock_connect.return_value = self._mock_connection

        ca_cert_data = "-----BEGIN CERTIFICATE-----\nCA_CERT_DATA\n-----END CERTIFICATE-----"
        credentials = {
            "connect_args": _ORACLE_DB_CREDENTIALS,
            "ssl_options": {
                "ca_data": ca_cert_data,
                "disabled": False,
            },
        }

        client = OracleProxyClient(credentials)

        # Verify SSL context was created with correct options
        mock_create_ssl_context.assert_called_once()
        ssl_options_arg = mock_create_ssl_context.call_args[0][0]
        self.assertEqual(ssl_options_arg.ca_data, ca_cert_data)
        self.assertFalse(ssl_options_arg.disabled)

        # Verify oracledb.connect was called with ssl_context
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args[1]
        self.assertIn("ssl_context", call_kwargs)
        self.assertEqual(call_kwargs["ssl_context"], mock_ssl_context)
        self.assertEqual(call_kwargs["expire_time"], 1)
        self.assertEqual(call_kwargs["dsn"], _ORACLE_DB_CREDENTIALS["dsn"])
        self.assertEqual(call_kwargs["user"], _ORACLE_DB_CREDENTIALS["user"])
        self.assertEqual(call_kwargs["password"], _ORACLE_DB_CREDENTIALS["password"])

        self.assertEqual(client.wrapped_client, self._mock_connection)

    @patch("oracledb.connect")
    @patch("apollo.integrations.db.oracle_proxy_client.create_oracle_ssl_context")
    def test_connect_with_ssl_mtls(
        self, mock_create_ssl_context: Mock, mock_connect: Mock
    ) -> None:
        """Test Oracle connection with SSL using CA cert and client cert/key (mTLS)."""
        mock_ssl_context = MagicMock(spec=ssl.SSLContext)
        mock_create_ssl_context.return_value = mock_ssl_context
        mock_connect.return_value = self._mock_connection

        ca_cert_data = "-----BEGIN CERTIFICATE-----\nCA_CERT_DATA\n-----END CERTIFICATE-----"
        client_cert_data = "-----BEGIN CERTIFICATE-----\nCLIENT_CERT_DATA\n-----END CERTIFICATE-----"
        client_key_data = "-----BEGIN PRIVATE KEY-----\nCLIENT_KEY_DATA\n-----END PRIVATE KEY-----"
        credentials = {
            "connect_args": _ORACLE_DB_CREDENTIALS,
            "ssl_options": {
                "ca_data": ca_cert_data,
                "cert_data": client_cert_data,
                "key_data": client_key_data,
                "key_password": None,
                "disabled": False,
            },
        }

        client = OracleProxyClient(credentials)

        # Verify SSL context was created with correct options including client cert
        mock_create_ssl_context.assert_called_once()
        ssl_options_arg = mock_create_ssl_context.call_args[0][0]
        self.assertEqual(ssl_options_arg.ca_data, ca_cert_data)
        self.assertEqual(ssl_options_arg.cert_data, client_cert_data)
        self.assertEqual(ssl_options_arg.key_data, client_key_data)
        self.assertFalse(ssl_options_arg.disabled)

        # Verify oracledb.connect was called with ssl_context
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args[1]
        self.assertIn("ssl_context", call_kwargs)
        self.assertEqual(call_kwargs["ssl_context"], mock_ssl_context)

        self.assertEqual(client.wrapped_client, self._mock_connection)

    @patch("oracledb.connect")
    @patch("apollo.integrations.db.oracle_proxy_client.create_oracle_ssl_context")
    def test_connect_with_ssl_disabled(
        self, mock_create_ssl_context: Mock, mock_connect: Mock
    ) -> None:
        """Test Oracle connection with SSL disabled."""
        mock_create_ssl_context.return_value = None  # SSL disabled returns None
        mock_connect.return_value = self._mock_connection

        credentials = {
            "connect_args": _ORACLE_DB_CREDENTIALS,
            "ssl_options": {
                "disabled": True,
                # Note: ca_data cannot be provided when disabled=True due to SslOptions validation
            },
        }

        client = OracleProxyClient(credentials)

        # Verify SSL context creation was attempted but returned None (no ca_data)
        mock_create_ssl_context.assert_called_once()
        ssl_options_arg = mock_create_ssl_context.call_args[0][0]
        self.assertTrue(ssl_options_arg.disabled)
        self.assertIsNone(ssl_options_arg.ca_data)

        # Verify oracledb.connect was called WITHOUT ssl_context
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args[1]
        self.assertNotIn("ssl_context", call_kwargs)
        self.assertEqual(call_kwargs["expire_time"], 1)

        self.assertEqual(client.wrapped_client, self._mock_connection)

    @patch("oracledb.connect")
    @patch("apollo.integrations.db.oracle_proxy_client.create_oracle_ssl_context")
    def test_connect_without_ssl_options(
        self, mock_create_ssl_context: Mock, mock_connect: Mock
    ) -> None:
        """Test Oracle connection without SSL options."""
        mock_create_ssl_context.return_value = None  # No ca_data returns None
        mock_connect.return_value = self._mock_connection

        credentials = {
            "connect_args": _ORACLE_DB_CREDENTIALS,
        }

        client = OracleProxyClient(credentials)

        # Verify SSL context creation was attempted but returned None (no ca_data)
        mock_create_ssl_context.assert_called_once()
        ssl_options_arg = mock_create_ssl_context.call_args[0][0]
        self.assertIsNone(ssl_options_arg.ca_data)

        # Verify oracledb.connect was called WITHOUT ssl_context
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args[1]
        self.assertNotIn("ssl_context", call_kwargs)
        self.assertEqual(call_kwargs["expire_time"], 1)

        self.assertEqual(client.wrapped_client, self._mock_connection)
