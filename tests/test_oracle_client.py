import datetime
import json
import logging
import os
import shutil
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
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.db import oracle_client_config
from apollo.integrations.db.oracle_client_config import (
    create_oracle_ssl_context,
    create_oracle_thick_wallet,
)
from apollo.integrations.db.oracle_proxy_client import OracleProxyClient
from apollo.integrations.db.base_db_proxy_client import SslOptions

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

        ca_cert_data = (
            "-----BEGIN CERTIFICATE-----\nCA_CERT_DATA\n-----END CERTIFICATE-----"
        )
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
    def test_ssl_options_inside_connect_args_is_popped(
        self, mock_create_ssl_context: Mock, mock_connect: Mock
    ) -> None:
        """The CTP delivers ssl_options inside connect_args. It must drive SSL and
        be removed before oracledb.connect (it is not an oracledb.connect arg)."""
        mock_create_ssl_context.return_value = MagicMock(spec=ssl.SSLContext)
        mock_connect.return_value = self._mock_connection

        OracleProxyClient(
            {
                "connect_args": {
                    **_ORACLE_DB_CREDENTIALS,
                    "ssl_options": {"ca_data": "CA_FROM_CTP"},
                }
            }
        )

        mock_create_ssl_context.assert_called_once()
        self.assertEqual(mock_create_ssl_context.call_args[0][0].ca_data, "CA_FROM_CTP")
        call_kwargs = mock_connect.call_args[1]
        self.assertNotIn("ssl_options", call_kwargs)  # popped, not sent to connect

    @patch("oracledb.connect")
    @patch("apollo.integrations.db.oracle_proxy_client.create_oracle_ssl_context")
    def test_connect_with_ssl_mtls(
        self, mock_create_ssl_context: Mock, mock_connect: Mock
    ) -> None:
        """Test Oracle connection with SSL using CA cert and client cert/key (mTLS)."""
        mock_ssl_context = MagicMock(spec=ssl.SSLContext)
        mock_create_ssl_context.return_value = mock_ssl_context
        mock_connect.return_value = self._mock_connection

        ca_cert_data = (
            "-----BEGIN CERTIFICATE-----\nCA_CERT_DATA\n-----END CERTIFICATE-----"
        )
        client_cert_data = (
            "-----BEGIN CERTIFICATE-----\nCLIENT_CERT_DATA\n-----END CERTIFICATE-----"
        )
        client_key_data = (
            "-----BEGIN PRIVATE KEY-----\nCLIENT_KEY_DATA\n-----END PRIVATE KEY-----"
        )
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


class OracleThickModeTests(TestCase):
    """Thick mode is enabled process-wide via the MCD_ORACLE_THICK_MODE env var."""

    def setUp(self) -> None:
        self._mock_connection = Mock()
        # Reset the process-wide thick-mode state so each test starts fresh.
        oracle_client_config._thick_config_dir = None
        oracle_client_config._thick_wallet_dir = None
        oracle_client_config._thick_ca_fingerprint = None

    @patch.dict("os.environ", {"MCD_ORACLE_THICK_MODE": "true"})
    @patch("oracledb.connect")
    @patch("oracledb.init_oracle_client")
    @patch("oracledb.is_thin_mode", return_value=True)
    def test_thick_mode_initializes_client(
        self, _mock_thin: Mock, mock_init: Mock, mock_connect: Mock
    ) -> None:
        mock_connect.return_value = self._mock_connection

        client = OracleProxyClient({"connect_args": dict(_ORACLE_DB_CREDENTIALS)})

        mock_init.assert_called_once()
        # config_dir carries the sqlnet.ora with SSL_CIPHER_SUITES for TLS.
        self.assertIn("config_dir", mock_init.call_args.kwargs)
        self.assertEqual(client.wrapped_client, self._mock_connection)

    @patch.dict("os.environ", {"MCD_ORACLE_THICK_MODE": "true"})
    @patch("oracledb.connect")
    @patch("oracledb.init_oracle_client")
    @patch("oracledb.is_thin_mode", return_value=False)
    def test_thick_mode_skips_init_when_already_thick(
        self, _mock_thin: Mock, mock_init: Mock, mock_connect: Mock
    ) -> None:
        """init_oracle_client() is process-global; only initialize once."""
        mock_connect.return_value = self._mock_connection

        OracleProxyClient({"connect_args": dict(_ORACLE_DB_CREDENTIALS)})

        mock_init.assert_not_called()

    @patch.dict("os.environ", {}, clear=True)
    @patch("oracledb.connect")
    @patch("oracledb.init_oracle_client")
    def test_thin_mode_does_not_initialize_client(
        self, mock_init: Mock, mock_connect: Mock
    ) -> None:
        """Without the env var, thick mode is never initialized."""
        mock_connect.return_value = self._mock_connection

        OracleProxyClient({"connect_args": dict(_ORACLE_DB_CREDENTIALS)})

        mock_init.assert_not_called()

    @patch.dict("os.environ", {"MCD_ORACLE_THICK_MODE": "true"})
    @patch("oracledb.connect")
    @patch("oracledb.init_oracle_client")
    @patch("oracledb.is_thin_mode", return_value=True)
    @patch("apollo.integrations.db.oracle_client_config._write_thick_sqlnet")
    @patch("apollo.integrations.db.oracle_client_config.create_oracle_thick_wallet")
    def test_thick_mode_with_ssl_builds_wallet(
        self,
        mock_wallet: Mock,
        mock_write_sqlnet: Mock,
        _mock_thin: Mock,
        mock_init: Mock,
        mock_connect: Mock,
    ) -> None:
        """Thick mode + ssl_options builds the wallet and wires it via sqlnet.ora
        WALLET_LOCATION before init_oracle_client — not the connect()
        wallet_location param (thick ignores it for trust) and not ssl_context
        (thin-only)."""
        mock_connect.return_value = self._mock_connection
        mock_wallet.return_value = "/tmp/mcd_oracle_wallet_test"

        OracleProxyClient(
            {
                "connect_args": dict(_ORACLE_DB_CREDENTIALS),
                "ssl_options": {
                    "ca_data": "-----BEGIN CERTIFICATE-----\nX\n-----END CERTIFICATE-----",
                    "disabled": False,
                },
            }
        )

        mock_wallet.assert_called_once()
        # Wallet wired through sqlnet.ora WALLET_LOCATION (positional args:
        # config_dir, wallet_dir, verify_identity), written BEFORE init.
        mock_write_sqlnet.assert_called_once()
        args = mock_write_sqlnet.call_args.args
        self.assertEqual(args[1], "/tmp/mcd_oracle_wallet_test")
        self.assertTrue(args[2])  # verify_identity default True
        mock_init.assert_called_once()
        # Trust comes from sqlnet.ora, so connect gets no wallet/ssl params.
        kwargs = mock_connect.call_args[1]
        self.assertNotIn("wallet_location", kwargs)
        self.assertNotIn("wallet_password", kwargs)
        self.assertNotIn("ssl_context", kwargs)  # ssl_context is thin-only

    @patch.dict("os.environ", {"MCD_ORACLE_THICK_MODE": "true"})
    @patch("oracledb.connect")
    @patch("oracledb.is_thin_mode", return_value=True)
    @patch(
        "apollo.integrations.db.oracle_client_config.create_oracle_thick_wallet",
        return_value="/tmp/mcd_oracle_wallet_test",
    )
    def test_thick_tls_wallet_written_before_init(
        self, _mock_wallet: Mock, _mock_thin: Mock, mock_connect: Mock
    ) -> None:
        """The wallet + WALLET_LOCATION must be in sqlnet.ora BEFORE
        init_oracle_client, or Oracle's wallet subsystem starts without trust."""
        mock_connect.return_value = self._mock_connection
        order: List[str] = []
        with patch(
            "apollo.integrations.db.oracle_client_config._write_thick_sqlnet",
            side_effect=lambda *a, **k: order.append("sqlnet"),
        ), patch(
            "oracledb.init_oracle_client",
            side_effect=lambda *a, **k: order.append("init"),
        ):
            OracleProxyClient(
                {
                    "connect_args": dict(_ORACLE_DB_CREDENTIALS),
                    "ssl_options": {"ca_data": "ca"},
                }
            )
        self.assertEqual(order, ["sqlnet", "init"])

    @patch.dict("os.environ", {"MCD_ORACLE_THICK_MODE": "true"})
    @patch("oracledb.connect")
    @patch("oracledb.init_oracle_client")
    @patch("oracledb.is_thin_mode", return_value=False)
    def test_thick_tls_raises_when_later_ca_differs(
        self, _mock_thin: Mock, mock_init: Mock, mock_connect: Mock
    ) -> None:
        """Thick trust is frozen after the first connection; a later connection
        with a different CA can't be applied — fail loudly, not silent ORA-29024."""
        mock_connect.return_value = self._mock_connection
        oracle_client_config._thick_wallet_dir = "/tmp/established_wallet"
        oracle_client_config._thick_ca_fingerprint = "established-fingerprint"

        with self.assertRaises(RuntimeError) as ctx:
            OracleProxyClient(
                {
                    "connect_args": dict(_ORACLE_DB_CREDENTIALS),
                    "ssl_options": {"ca_data": "a-different-ca"},
                }
            )
        self.assertIn("different SSL CA", str(ctx.exception))
        mock_init.assert_not_called()  # already initialized
        mock_connect.assert_not_called()  # never connects with unappliable trust

    @patch.dict("os.environ", {"MCD_ORACLE_THICK_MODE": "true"})
    @patch("oracledb.connect")
    @patch("oracledb.init_oracle_client")
    @patch("oracledb.is_thin_mode", return_value=False)
    def test_thick_tls_raises_when_process_inited_without_tls(
        self, _mock_thin: Mock, mock_init: Mock, mock_connect: Mock
    ) -> None:
        """An SSL connection arriving after the process was initialized without a
        wallet (e.g. a prior non-SSL connection warmed the container) can't have
        trust applied — fail loudly."""
        mock_connect.return_value = self._mock_connection
        # setUp reset _thick_wallet_dir to None (inited without TLS).
        with self.assertRaises(RuntimeError) as ctx:
            OracleProxyClient(
                {
                    "connect_args": dict(_ORACLE_DB_CREDENTIALS),
                    "ssl_options": {"ca_data": "some-ca"},
                }
            )
        self.assertIn("without an SSL trust store", str(ctx.exception))
        mock_connect.assert_not_called()


class CreateOracleSslContextTests(TestCase):
    """Tests for the create_oracle_ssl_context function"""

    @patch("ssl.SSLContext")
    def test_create_ssl_context_default_verification(
        self, mock_ssl_context_class: Mock
    ):
        """Test SSL context creation with default verification settings (all enabled)"""
        mock_ctx = MagicMock()
        mock_ssl_context_class.return_value = mock_ctx

        ssl_options = SslOptions(
            ca_data="-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----",
            skip_cert_verification=False,
            verify_cert=True,
            verify_identity=True,
        )

        result = create_oracle_ssl_context(ssl_options)

        mock_ssl_context_class.assert_called_with(ssl.PROTOCOL_TLS_CLIENT)
        # With default settings, both hostname check and cert verification are enabled
        self.assertTrue(mock_ctx.check_hostname)
        self.assertEqual(mock_ctx.verify_mode, ssl.CERT_REQUIRED)
        mock_ctx.set_ciphers.assert_called_with("DEFAULT:@SECLEVEL=1")
        mock_ctx.load_verify_locations.assert_called_once()
        self.assertEqual(result, mock_ctx)

    @patch("ssl.SSLContext")
    def test_create_ssl_context_verify_identity_false(
        self, mock_ssl_context_class: Mock
    ):
        """Test SSL context creation with verify_identity=False (hostname check disabled)"""
        mock_ctx = MagicMock()
        mock_ssl_context_class.return_value = mock_ctx

        ssl_options = SslOptions(
            ca_data="-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----",
            skip_cert_verification=False,
            verify_cert=True,
            verify_identity=False,
        )

        result = create_oracle_ssl_context(ssl_options)

        # Hostname check disabled, but cert verification still enabled
        self.assertFalse(mock_ctx.check_hostname)
        self.assertEqual(mock_ctx.verify_mode, ssl.CERT_REQUIRED)
        mock_ctx.load_verify_locations.assert_called_once()

    @patch("ssl.SSLContext")
    def test_create_ssl_context_skip_cert_verification(
        self, mock_ssl_context_class: Mock
    ):
        """Test SSL context creation with skip_cert_verification=True (all verification disabled)"""
        mock_ctx = MagicMock()
        mock_ssl_context_class.return_value = mock_ctx

        ssl_options = SslOptions(
            ca_data="-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----",
            skip_cert_verification=True,
            verify_cert=True,
            verify_identity=True,
        )

        result = create_oracle_ssl_context(ssl_options)

        # Both hostname check and cert verification disabled
        self.assertFalse(mock_ctx.check_hostname)
        self.assertEqual(mock_ctx.verify_mode, ssl.CERT_NONE)
        # CA should NOT be loaded when skipping verification
        mock_ctx.load_verify_locations.assert_not_called()

    @patch("ssl.SSLContext")
    def test_create_ssl_context_verify_cert_false(self, mock_ssl_context_class: Mock):
        """Test SSL context creation with verify_cert=False"""
        mock_ctx = MagicMock()
        mock_ssl_context_class.return_value = mock_ctx

        ssl_options = SslOptions(
            ca_data="-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----",
            skip_cert_verification=False,
            verify_cert=False,
            verify_identity=True,
        )

        result = create_oracle_ssl_context(ssl_options)

        # Cert verification disabled
        self.assertEqual(mock_ctx.verify_mode, ssl.CERT_NONE)
        # Hostname check still respects verify_identity
        self.assertTrue(mock_ctx.check_hostname)

    def test_create_ssl_context_disabled_returns_none(self):
        """Test that disabled SSL returns None"""
        ssl_options = SslOptions(disabled=True)
        result = create_oracle_ssl_context(ssl_options)
        self.assertIsNone(result)

    def test_create_ssl_context_no_ca_data_returns_none(self):
        """Test that missing CA data returns None"""
        ssl_options = SslOptions(ca_data=None)
        result = create_oracle_ssl_context(ssl_options)
        self.assertIsNone(result)


class _ListHandler(logging.Handler):
    def __init__(self, records):
        super().__init__()
        self._records = records

    def emit(self, record):
        self._records.append(record)


class OracleCtpCredentialSafetyTests(TestCase):
    """CTP connection errors must be actionable without leaking credentials."""

    _HOST = "db.example.com"
    _DSN = "db.example.com:1521/ORCL"
    _USER = "svc_account@example.com"
    _PASSWORD = "s3cr3t_p@ssw0rd!"

    _OPERATION = {
        "trace_id": "ctp-safety-test",
        "skip_cache": True,
        "commands": [
            {"method": "cursor", "store": "_cursor"},
            {
                "target": "_cursor",
                "method": "execute",
                "args": ["SELECT 1 FROM DUAL", None],
            },
        ],
    }

    def setUp(self):
        self._agent = Agent(LoggingUtils())
        self._log_records = []
        self._log_handler = _ListHandler(self._log_records)
        logging.getLogger().addHandler(self._log_handler)

    def tearDown(self):
        logging.getLogger().removeHandler(self._log_handler)

    def _assert_no_credential_leak(self, response) -> None:
        serialized = json.dumps(response.result, default=str)
        self.assertNotIn(self._PASSWORD, serialized, "password leaked in response")
        self.assertNotIn(self._USER, serialized, "username leaked in response")

    @patch("oracledb.connect")
    def test_connect_failure_is_actionable_and_safe(self, mock_connect):
        """Connection failure exposes the DSN but not the password."""
        mock_connect.side_effect = Exception(
            f"ORA-12541: TNS:No listener at {self._HOST}:1521"
        )
        response = self._agent.execute_operation(
            "oracle",
            "run_query",
            self._OPERATION,
            {
                "dsn": self._DSN,
                "user": self._USER,
                "password": self._PASSWORD,
            },
        )
        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn(self._HOST, error)
        self._assert_no_credential_leak(response)

    @patch("oracledb.connect")
    def test_auth_failure_is_actionable_and_safe(self, mock_connect):
        """Auth failure surfaces a useful error without leaking credentials."""
        mock_connect.side_effect = Exception(
            "ORA-01017: invalid username/password; logon denied"
        )
        response = self._agent.execute_operation(
            "oracle",
            "run_query",
            self._OPERATION,
            {
                "dsn": self._DSN,
                "user": self._USER,
                "password": self._PASSWORD,
            },
        )
        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn("ORA-01017", error)
        self._assert_no_credential_leak(response)

    @patch("oracledb.connect")
    def test_log_output_does_not_leak_credentials(self, mock_connect):
        """JsonLogFormatter (Datadog/Lambda path) never emits the password."""
        from apollo.interfaces.lambda_function.json_log_formatter import (
            JsonLogFormatter,
        )

        mock_connect.side_effect = Exception(f"Failed to connect to {self._DSN}")
        self._agent.execute_operation(
            "oracle",
            "run_query",
            self._OPERATION,
            {
                "dsn": self._DSN,
                "user": self._USER,
                "password": self._PASSWORD,
            },
        )
        formatter = JsonLogFormatter()
        for record in self._log_records:
            output = formatter.format(record)
            self.assertNotIn(self._PASSWORD, output)


class CreateOracleThickWalletTests(TestCase):
    """create_oracle_thick_wallet builds a cwallet.sso via orapki. orapki needs the
    bundled JRE, so it is mocked here (asserting the orapki commands issued); the
    real wallet build + TLS handshake is covered by the E2E rig against RDS."""

    @staticmethod
    def _self_signed(cn: str):
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import rsa

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
        cert = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime(2020, 1, 1))
            .not_valid_after(datetime.datetime(2035, 1, 1))
            .sign(key, hashes.SHA256())
        )
        return key, cert

    @classmethod
    def _pem_cert(cls, cert) -> str:
        from cryptography.hazmat.primitives import serialization

        return cert.public_bytes(serialization.Encoding.PEM).decode()

    @classmethod
    def _pem_key(cls, key) -> str:
        from cryptography.hazmat.primitives import serialization

        return key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode()

    @patch("apollo.integrations.db.oracle_client_config._run_orapki")
    def test_returns_none_when_disabled_or_no_ca(self, mock_orapki: Mock):
        self.assertIsNone(create_oracle_thick_wallet(SslOptions(disabled=True)))
        self.assertIsNone(create_oracle_thick_wallet(SslOptions(ca_data=None)))
        mock_orapki.assert_not_called()

    @patch("apollo.integrations.db.oracle_client_config._run_orapki")
    def test_one_way_builds_auto_login_wallet_with_trusted_ca(self, mock_orapki: Mock):
        _, ca = self._self_signed("Test CA")
        wallet_dir = create_oracle_thick_wallet(SslOptions(ca_data=self._pem_cert(ca)))
        self.assertIsNotNone(wallet_dir)
        self.addCleanup(shutil.rmtree, wallet_dir, ignore_errors=True)
        cmds = [c.args for c in mock_orapki.call_args_list]
        # wallet created as auto-login (produces cwallet.sso, the only form thick trusts)
        self.assertTrue(
            any(a[:2] == ("wallet", "create") and "-auto_login" in a for a in cmds)
        )
        # CA added as a trusted certificate
        self.assertTrue(
            any(a[:2] == ("wallet", "add") and "-trusted_cert" in a for a in cmds)
        )
        # no client identity import for one-way TLS
        self.assertFalse(any("import_pkcs12" in a for a in cmds))

    @patch("apollo.integrations.db.oracle_client_config._run_orapki")
    def test_mtls_imports_client_identity(self, mock_orapki: Mock):
        _, ca = self._self_signed("Test CA")
        cli_key, cli_cert = self._self_signed("client")
        wallet_dir = create_oracle_thick_wallet(
            SslOptions(
                ca_data=self._pem_cert(ca),
                cert_data=self._pem_cert(cli_cert),
                key_data=self._pem_key(cli_key),
            )
        )
        self.assertIsNotNone(wallet_dir)
        self.addCleanup(shutil.rmtree, wallet_dir, ignore_errors=True)
        cmds = [c.args for c in mock_orapki.call_args_list]
        self.assertTrue(any(a[:2] == ("wallet", "import_pkcs12") for a in cmds))

    @patch("apollo.integrations.db.oracle_client_config._run_orapki")
    def test_wallet_dir_removed_on_failure(self, mock_orapki: Mock):
        captured: dict = {}

        def boom(*args: str) -> None:
            # first call is "wallet create -wallet <dir> ..."; record the dir
            captured["dir"] = args[args.index("-wallet") + 1]
            raise RuntimeError("orapki boom")

        mock_orapki.side_effect = boom
        _, ca = self._self_signed("Test CA")
        with self.assertRaises(RuntimeError):
            create_oracle_thick_wallet(SslOptions(ca_data=self._pem_cert(ca)))
        # the partially-built wallet dir is cleaned up rather than left behind
        self.assertTrue(captured.get("dir"))
        self.assertFalse(os.path.exists(captured["dir"]))
