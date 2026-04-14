import datetime
from typing import List, Any, Optional
from unittest import TestCase
from unittest.mock import Mock, call, patch

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.ctp.registry import CtpRegistry
from apollo.integrations.db.fabric_proxy_client import MsFabricProxyClient

_HOST = "myworkspace.datawarehouse.fabric.microsoft.com"
_PORT = 1433
_DATABASE = "mydb"
_CLIENT_ID = "my-client-id"
_CLIENT_SECRET = "my-client-secret"
_TENANT_ID = "my-tenant-id"

# Flat DC credentials — raw input to the CTP pipeline.
_FLAT_CREDS = {
    "server": _HOST,
    "database": _DATABASE,
    "client_id": _CLIENT_ID,
    "client_secret": _CLIENT_SECRET,
    "tenant_id": _TENANT_ID,
}

# Post-CTP ODBC dict: connect_args_defaults keys first, then mapper output.
# This is what MsFabricProxyClient receives after CtpRegistry.resolve().
_CONNECT_ARGS_DICT = {
    # From connect_args_defaults
    "DRIVER": "{ODBC Driver 17 for SQL Server}",
    "Authentication": "ActiveDirectoryServicePrincipal",
    "Encrypt": "yes",
    "TrustServerCertificate": "no",
    # From mapper
    "SERVER": f"{_HOST},{_PORT}",
    "DATABASE": _DATABASE,
    "UID": f"{_CLIENT_ID}@{_TENANT_ID}",
    "PWD": _CLIENT_SECRET,
}

# ODBC string produced by odbc_string_from_dict(_CONNECT_ARGS_DICT).
# Values in _CONNECT_ARGS_DICT have no special chars that need escaping, so
# a plain join matches what odbc_string_from_dict produces.
_EXPECTED_ODBC_STRING = ";".join(f"{k}={v}" for k, v in _CONNECT_ARGS_DICT.items())


class MsFabricProxyClientTests(TestCase):
    """Unit tests for MsFabricProxyClient — pass the post-CTP ODBC dict directly."""

    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor
        self.maxDiff = None

    @patch("pyodbc.connect")
    def test_connect_args_dict_serialized_to_odbc_string(self, mock_connect):
        """connect_args as ODBC dict → serialized connection string passed to pyodbc.connect."""
        mock_connect.return_value = self._mock_connection
        MsFabricProxyClient(
            credentials={"connect_args": _CONNECT_ARGS_DICT},
            platform="test",
        )
        mock_connect.assert_called_once_with(_EXPECTED_ODBC_STRING, timeout=15)

    def test_connect_args_string_raises(self):
        """connect_args as a string is not accepted — must be a dict."""
        with self.assertRaises(ValueError):
            MsFabricProxyClient(
                credentials={"connect_args": _EXPECTED_ODBC_STRING},
                platform="test",
            )

    @patch("pyodbc.connect")
    def test_login_timeout_from_connect_args(self, mock_connect):
        """login_timeout in connect_args overrides the default and is not serialized to the ODBC string."""
        mock_connect.return_value = self._mock_connection
        MsFabricProxyClient(
            credentials={"connect_args": {**_CONNECT_ARGS_DICT, "login_timeout": 30}},
            platform="test",
        )
        mock_connect.assert_called_once_with(_EXPECTED_ODBC_STRING, timeout=30)

    @patch("pyodbc.connect")
    def test_query_timeout_from_connect_args(self, mock_connect):
        """query_timeout_in_seconds in connect_args overrides the default and is not serialized to the ODBC string."""
        mock_connect.return_value = self._mock_connection
        client = MsFabricProxyClient(
            credentials={
                "connect_args": {**_CONNECT_ARGS_DICT, "query_timeout_in_seconds": 120},
            },
            platform="test",
        )
        self.assertEqual(120, client.wrapped_client.timeout)
        self.assertNotIn("query_timeout_in_seconds", mock_connect.call_args[0][0])

    def test_missing_connect_args_raises(self):
        """Missing connect_args raises ValueError."""
        with self.assertRaises(ValueError):
            MsFabricProxyClient(credentials={}, platform="test")

    def test_none_credentials_raises(self):
        """None credentials raises ValueError."""
        with self.assertRaises(ValueError):
            MsFabricProxyClient(credentials=None, platform="test")

    @patch("pyodbc.connect")
    def test_invalid_connect_args_type_raises(self, mock_connect):
        """connect_args of unexpected type raises ValueError."""
        mock_connect.return_value = self._mock_connection
        with self.assertRaises(ValueError):
            MsFabricProxyClient(credentials={"connect_args": 12345}, platform="test")

    @patch("pyodbc.connect")
    def test_connect_failure_propagates(self, mock_connect):
        """pyodbc.OperationalError raised during connect propagates to the caller."""
        import pyodbc as _pyodbc

        mock_connect.side_effect = _pyodbc.OperationalError("connection refused")
        with self.assertRaises(_pyodbc.OperationalError):
            MsFabricProxyClient(
                credentials={"connect_args": _CONNECT_ARGS_DICT},
                platform="test",
            )

    @patch("pyodbc.connect")
    def test_dict_value_with_semicolon_is_escaped(self, mock_connect):
        """connect_args dict values containing semicolons are brace-escaped in the ODBC string."""
        mock_connect.return_value = self._mock_connection
        tricky_secret = "p@ss;word=1"
        creds = {**_CONNECT_ARGS_DICT, "PWD": tricky_secret}
        MsFabricProxyClient(credentials={"connect_args": creds}, platform="test")
        call_args = mock_connect.call_args[0][0]
        self.assertIn("PWD={p@ss;word=1}", call_args)
        self.assertNotIn("PWD=p@ss;word=1", call_args)

    @patch("pyodbc.connect")
    def test_query_via_agent(self, mock_connect):
        """End-to-end query through the Agent using flat credentials (CTP resolves automatically)."""
        query = "SELECT name, value FROM dbo.table"
        expected_data = [["foo", 1], ["bar", 2]]
        expected_description = [
            ["name", str.__class__, None, None, None, None, None],
            ["value", int.__class__, None, None, None, None, None],
        ]
        self._test_run_query(
            mock_connect, query, None, expected_data, expected_description
        )

    def _test_run_query(
        self,
        mock_connect: Mock,
        query: str,
        query_args: Optional[List[Any]],
        data: List,
        description: List,
    ):
        operation_dict = {
            "trace_id": "1234",
            "skip_cache": True,
            "commands": [
                {"method": "cursor", "store": "_cursor"},
                {
                    "target": "_cursor",
                    "method": "execute",
                    "args": [query, query_args],
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
        self._mock_cursor.fetchall.return_value = data
        # description and rowcount are pyodbc attributes, not callables — set directly
        self._mock_cursor.description = description
        self._mock_cursor.rowcount = len(data)

        response = self._agent.execute_operation(
            "microsoft-fabric",
            "run_query",
            operation_dict,
            _FLAT_CREDS,
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertIn(ATTRIBUTE_NAME_RESULT, response.result)
        result = response.result[ATTRIBUTE_NAME_RESULT]

        mock_connect.assert_called_with(_EXPECTED_ODBC_STRING, timeout=15)
        self._mock_cursor.execute.assert_has_calls([call(query, query_args)])
        self.assertEqual(data, result["all_results"])


class MsFabricCtpRoundTripTests(TestCase):
    """Verify the CTP pipeline produces the expected ODBC dict from flat credentials."""

    def test_ctp_registered(self):
        self.assertIsNotNone(CtpRegistry.get("microsoft-fabric"))

    def test_ctp_resolves_flat_credentials(self):
        resolved = CtpRegistry.resolve("microsoft-fabric", _FLAT_CREDS)
        self.assertEqual(_CONNECT_ARGS_DICT, resolved["connect_args"])

    @patch("pyodbc.connect")
    def test_ctp_to_proxy_client_end_to_end(self, mock_connect):
        """Flat credentials → CTP → proxy client → correct pyodbc call."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection

        resolved = CtpRegistry.resolve("microsoft-fabric", _FLAT_CREDS)
        MsFabricProxyClient(credentials=resolved, platform="test")

        mock_connect.assert_called_once_with(_EXPECTED_ODBC_STRING, timeout=15)

    def test_ctp_resolves_host_alias(self):
        """host and hostname are accepted as aliases for server."""
        for key in ("host", "hostname"):
            with self.subTest(key=key):
                creds = {**_FLAT_CREDS, key: _HOST}
                creds.pop("server")
                resolved = CtpRegistry.resolve("microsoft-fabric", creds)
                self.assertEqual(f"{_HOST},{_PORT}", resolved["connect_args"]["SERVER"])

    def test_ctp_resolves_custom_port(self):
        """A non-default port is included in the SERVER field."""
        creds = {**_FLAT_CREDS, "port": 1234}
        resolved = CtpRegistry.resolve("microsoft-fabric", creds)
        self.assertEqual(f"{_HOST},1234", resolved["connect_args"]["SERVER"])

    def test_ctp_resolves_db_name_alias(self):
        """db_name is accepted as an alias for database."""
        creds = {**_FLAT_CREDS, "db_name": _DATABASE}
        creds.pop("database")
        resolved = CtpRegistry.resolve("microsoft-fabric", creds)
        self.assertEqual(_DATABASE, resolved["connect_args"]["DATABASE"])

    def test_connect_args_defaults_inherited_by_custom_ctp(self):
        """A custom CTP that only maps dynamic fields still gets the static Fabric defaults."""
        custom_ctp = {
            "mapper": {
                "field_map": {
                    "SERVER": "{{ raw.server }},{{ raw.port | default(1433) }}",
                    "DATABASE": "{{ raw.database }}",
                    "UID": "{{ raw.client_id }}@{{ raw.tenant_id }}",
                    "PWD": "{{ raw.client_secret }}",
                }
            }
        }
        resolved = CtpRegistry.resolve_custom(
            "microsoft-fabric", _FLAT_CREDS, custom_ctp
        )
        connect_args = resolved["connect_args"]
        self.assertEqual("{ODBC Driver 17 for SQL Server}", connect_args["DRIVER"])
        self.assertEqual(
            "ActiveDirectoryServicePrincipal", connect_args["Authentication"]
        )
        self.assertEqual("yes", connect_args["Encrypt"])
        self.assertEqual("no", connect_args["TrustServerCertificate"])


class MsFabricDatetimeoffsetTests(TestCase):
    def test_handle_datetimeoffset(self):
        # 2025-12-10T12:32:10.000019+01:00 represented as binary
        datetimeoffset_as_binary = (
            b"\xe9\x07\x0c\x00\n\x00\x0c\x00 \x00\n\x008J\x00\x00\x01\x00\x00\x00"
        )

        expected_datetime = datetime.datetime(
            year=2025,
            month=12,
            day=10,
            hour=12,
            minute=32,
            second=10,
            microsecond=19,
            tzinfo=datetime.timezone(datetime.timedelta(hours=1, minutes=0)),
        )

        result = MsFabricProxyClient._handle_datetimeoffset(datetimeoffset_as_binary)
        self.assertEqual(expected_datetime, result)
