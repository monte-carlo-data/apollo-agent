import datetime
import json
import logging
import os
import tempfile
from typing import List, Any, Optional
from unittest import TestCase
from unittest.mock import Mock, call, patch
from psycopg2.errors import InsufficientPrivilege  # noqa

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.ctp.registry import CtpRegistry
from apollo.integrations.db.postgres_proxy_client import PostgresProxyClient

_POSTGRES_CREDENTIALS = {
    "host": "www.test.com",
    "user": "u",
    "password": "p",
    "port": 5432,
    "dbname": "db1",
}

_POSTGRES_FLAT_CREDENTIALS = {
    "host": "www.test.com",
    "user": "u",
    "password": "p",
    "port": "5432",
    "database": "db1",
}

# Expected connect_args after CTP resolves _POSTGRES_FLAT_CREDENTIALS (flat path)
# or passes through _POSTGRES_CREDENTIALS unchanged (DC connect_args path).
_EXPECTED_POSTGRES_CONNECT_ARGS = {
    "host": "www.test.com",
    "port": 5432,
    "user": "u",
    "password": "p",
    "dbname": "db1",
}


class PostgresClientTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor

    @patch("psycopg2.connect")
    def test_query(self, mock_connect):
        query = "SELECT name, value FROM table"  # noqa
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
            ["name", "string", None, None, None, None, None],
            ["value", "float", None, None, None, None, None],
        ]
        self._test_run_query(mock_connect, query, expected_data, expected_description)

    @patch("psycopg2.connect")
    def test_datetime_query(self, mock_connect):
        query = "SELECT name, created_date, updated_datetime FROM table"  # noqa
        data = [
            [
                "name_1",
                datetime.date.fromisoformat("2023-11-01"),
                datetime.datetime.fromisoformat("2023-11-01T10:59:00"),
            ],
        ]
        description = [
            ["name", "string", None, None, None, None, None],
            ["created_date", "date", None, None, None, None, None],
            ["updated_datetime", "date", None, None, None, None, None],
        ]
        self._test_run_query(mock_connect, query, data, description)

    @patch("psycopg2.connect")
    def test_privilege_error(self, mock_connect):
        query = ""
        data = []
        description = []
        self._test_run_query(
            mock_connect,
            query,
            data,
            description,
            raise_exception=InsufficientPrivilege("insufficient privilege"),
            expected_error_type="InsufficientPrivilege",
        )

    def _test_run_query(
        self,
        mock_connect: Mock,
        query: str,
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
        mock_connect.return_value = self._mock_connection

        expected_rows = len(data)

        if raise_exception:
            self._mock_cursor.execute.side_effect = raise_exception
        self._mock_cursor.fetchall.return_value = data
        self._mock_cursor.description.return_value = description
        self._mock_cursor.rowcount.return_value = expected_rows

        response = self._agent.execute_operation(
            "postgres",
            "run_query",
            operation_dict,
            {
                "connect_args": _POSTGRES_CREDENTIALS,
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

        mock_connect.assert_called_with(
            **_EXPECTED_POSTGRES_CONNECT_ARGS,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        self._mock_cursor.execute.assert_has_calls(
            [
                call(query, None),
            ]
        )
        self._mock_cursor.description.assert_called()
        self._mock_cursor.rowcount.assert_called()

        expected_data = self._serialized_data(data)
        self.assertTrue("all_results" in result)
        self.assertEqual(expected_data, result["all_results"])

        self.assertTrue("description" in result)
        self.assertEqual(description, result["description"])

        self.assertTrue("rowcount" in result)
        self.assertEqual(expected_rows, result["rowcount"])

    @classmethod
    def _serialized_data(cls, data: List) -> List:
        return [cls._serialized_row(v) for v in data]

    @classmethod
    def _serialized_row(cls, row: List) -> List:
        return [cls._serialized_value(v) for v in row]

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
        else:
            return value


class PostgresCtpPathTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor

    @patch("psycopg2.connect")
    def test_ctp_path_resolves_flat_credentials(self, mock_connect):
        """Flat credentials are resolved by CTP inside _create_proxy_client before reaching PostgresProxyClient."""
        mock_connect.return_value = self._mock_connection
        self._mock_cursor.fetchall.return_value = []
        self._mock_cursor.description.return_value = []
        self._mock_cursor.rowcount.return_value = 0

        operation_dict = {
            "trace_id": "ctp-test",
            "skip_cache": True,
            "commands": [
                {"method": "cursor", "store": "_cursor"},
                {"target": "_cursor", "method": "execute", "args": ["SELECT 1", None]},
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
        # CTP runs inside _create_proxy_client — pass flat credentials directly
        self._agent.execute_operation(
            "postgres", "run_query", operation_dict, _POSTGRES_FLAT_CREDENTIALS
        )

        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args.kwargs
        self.assertEqual("www.test.com", call_kwargs["host"])
        self.assertEqual("u", call_kwargs["user"])
        self.assertEqual("db1", call_kwargs["dbname"])  # CTP mapped database → dbname
        self.assertEqual(1, call_kwargs["keepalives"])


class PostgresCredentialShapeTests(TestCase):
    """Verify PostgresProxyClient __init__ accepts both DC-style and CTP-resolved credentials.

    setUp/tearDown register and unregister the postgres CTP config so _ctp_creds() can
    call CtpRegistry.resolve() without depending on Phase 2 registration.

    DC path (today): DC plugin writes SSL cert files and builds connect_args with driver-native
    key names (dbname, sslrootcert, sslmode), then sends them to the agent.

    CTP path (after Phase 2): flat credentials go through CTP, which maps field names and
    materialises SSL certs before PostgresProxyClient is created.

    In both paths psycopg2.connect must receive the same effective arguments.
    """

    _HOST = "db.example.com"
    _PORT_STR = "5432"
    _USER = "admin"
    _PASSWORD = "secret"
    _CA_PEM = "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----"

    def setUp(self) -> None:
        pass  # postgres registered via _discover() in Phase 2

    def _dc_creds(self, **extra_connect_args):
        """Build DC-style credentials: connect_args with driver-native key names and int port."""
        return {
            "connect_args": {
                "host": self._HOST,
                "port": int(self._PORT_STR),
                "dbname": "mydb",
                "user": self._USER,
                "password": self._PASSWORD,
                **extra_connect_args,
            }
        }

    def _ctp_creds(self, **flat_kwargs):
        """Build CTP-resolved credentials from flat input via the registry."""
        return CtpRegistry.resolve(
            "postgres",
            {
                "host": self._HOST,
                "port": self._PORT_STR,
                "database": "mydb",
                "user": self._USER,
                "password": self._PASSWORD,
                **flat_kwargs,
            },
        )

    # ------------------------------------------------------------------
    # No SSL
    # ------------------------------------------------------------------

    @patch("psycopg2.connect")
    def test_dc_no_ssl(self, mock_connect):
        """DC sends connect_args without SSL — passed through to psycopg2 with keepalives."""
        mock_connect.return_value = Mock()
        PostgresProxyClient(credentials=self._dc_creds(), client_type="postgres")

        kwargs = mock_connect.call_args.kwargs
        self.assertEqual(self._HOST, kwargs["host"])
        self.assertEqual("mydb", kwargs["dbname"])
        self.assertNotIn("sslmode", kwargs)
        self.assertNotIn("sslrootcert", kwargs)
        self.assertEqual(1, kwargs["keepalives"])

    @patch("psycopg2.connect")
    def test_ctp_no_ssl(self, mock_connect):
        """CTP with no ssl_options — no SSL fields passed to psycopg2."""
        mock_connect.return_value = Mock()
        PostgresProxyClient(credentials=self._ctp_creds(), client_type="postgres")

        kwargs = mock_connect.call_args.kwargs
        self.assertEqual(self._HOST, kwargs["host"])
        self.assertEqual("mydb", kwargs["dbname"])
        self.assertNotIn("sslmode", kwargs)
        self.assertNotIn("sslrootcert", kwargs)

    # ------------------------------------------------------------------
    # CA data — cert written to file, sslrootcert=<path>
    # ------------------------------------------------------------------

    @patch("psycopg2.connect")
    def test_dc_ca_data(self, mock_connect):
        """DC writes cert to a path and sends sslrootcert + sslmode in connect_args."""
        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False, mode="w") as f:
            f.write(self._CA_PEM)
            cert_path = f.name
        try:
            mock_connect.return_value = Mock()
            PostgresProxyClient(
                credentials=self._dc_creds(sslrootcert=cert_path, sslmode="require"),
                client_type="postgres",
            )
            kwargs = mock_connect.call_args.kwargs
            self.assertEqual(cert_path, kwargs["sslrootcert"])
            self.assertEqual("require", kwargs["sslmode"])
        finally:
            os.unlink(cert_path)

    @patch("psycopg2.connect")
    def test_ctp_ca_data(self, mock_connect):
        """CTP resolves ssl_options ca_data to sslrootcert=<path> and sslmode=require."""
        mock_connect.return_value = Mock()
        PostgresProxyClient(
            credentials=self._ctp_creds(ssl_options={"ca_data": self._CA_PEM}),
            client_type="postgres",
        )
        kwargs = mock_connect.call_args.kwargs
        cert_path = kwargs.get("sslrootcert")
        self.assertIsInstance(cert_path, str)
        self.assertTrue(os.path.exists(cert_path))
        self.assertEqual("require", kwargs["sslmode"])
        os.unlink(cert_path)


class _ListHandler(logging.Handler):
    def __init__(self, records):
        super().__init__()
        self._records = records

    def emit(self, record):
        self._records.append(record)


class PostgresCtpCredentialSafetyTests(TestCase):
    """CTP validation and connection errors must be actionable without leaking credentials."""

    _HOST = "db.example.com"
    _USER = "admin@example.com"
    _PASSWORD = "s3cr3t_p@ssw0rd!"

    _OPERATION = {
        "trace_id": "ctp-safety-test",
        "skip_cache": True,
        "commands": [
            {"method": "cursor", "store": "_cursor"},
            {"target": "_cursor", "method": "execute", "args": ["SELECT 1", None]},
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

    @patch("psycopg2.connect")
    def test_missing_required_host_is_actionable_and_safe(self, mock_connect):
        """CTP validation: missing host produces an actionable error without leaking creds."""
        response = self._agent.execute_operation(
            "postgres",
            "run_query",
            self._OPERATION,
            # host intentionally omitted — Required[str] in schema
            {
                "port": "5432",
                "database": "mydb",
                "user": self._USER,
                "password": self._PASSWORD,
            },
        )
        mock_connect.assert_not_called()
        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn("host", error)
        self._assert_no_credential_leak(response)

    @patch("psycopg2.connect")
    def test_connect_failure_is_actionable_and_safe(self, mock_connect):
        """Connection failure exposes the hostname but not the password."""
        mock_connect.side_effect = Exception(
            f"could not connect to server: {self._HOST}:5432"
        )
        response = self._agent.execute_operation(
            "postgres",
            "run_query",
            self._OPERATION,
            {
                "host": self._HOST,
                "port": "5432",
                "database": "mydb",
                "user": self._USER,
                "password": self._PASSWORD,
            },
        )
        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn(self._HOST, error)
        self._assert_no_credential_leak(response)

    @patch("psycopg2.connect")
    def test_auth_failure_is_actionable_and_safe(self, mock_connect):
        """Auth failure surfaces a useful error without leaking credentials."""
        mock_connect.side_effect = Exception(
            "FATAL: password authentication failed for user"
        )
        response = self._agent.execute_operation(
            "postgres",
            "run_query",
            self._OPERATION,
            {
                "host": self._HOST,
                "port": "5432",
                "database": "mydb",
                "user": self._USER,
                "password": self._PASSWORD,
            },
        )
        self.assertIn(ATTRIBUTE_NAME_ERROR, response.result)
        error = response.result.get(ATTRIBUTE_NAME_ERROR, "")
        self.assertIn("authentication failed", error)
        self._assert_no_credential_leak(response)

    @patch("psycopg2.connect")
    def test_log_output_does_not_leak_credentials(self, mock_connect):
        """JsonLogFormatter (Datadog/Lambda path) never emits the password."""
        from apollo.interfaces.lambda_function.json_log_formatter import (
            JsonLogFormatter,
        )

        mock_connect.side_effect = Exception(f"could not connect to {self._HOST}")
        self._agent.execute_operation(
            "postgres",
            "run_query",
            self._OPERATION,
            {
                "host": self._HOST,
                "port": "5432",
                "database": "mydb",
                "user": self._USER,
                "password": self._PASSWORD,
            },
        )
        formatter = JsonLogFormatter()
        for record in self._log_records:
            output = formatter.format(record)
            self.assertNotIn(self._PASSWORD, output)
