import datetime
from typing import (
    List,
    Any,
    Optional,
)
from unittest import TestCase
from unittest.mock import (
    ANY,
    Mock,
    call,
    patch,
)

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR_TYPE,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.ctp.defaults.starburst_galaxy import (
    STARBURST_GALAXY_DEFAULT_CTP,
)
from apollo.integrations.ctp.registry import CtpRegistry
from apollo.integrations.db.starburst_proxy_client import StarburstProxyClient

_STARBURST_CREDENTIALS = {
    "host": "example.starburst.io",
    "port": "443",
    "http_scheme": "https",
    "catalog": "fizz",
    "schema": "buzz",
    "user": "foo",
    "password": "bar",
}
_EXPECTED_STARBURST_CREDENTIALS = {
    "host": "example.starburst.io",
    "port": "443",
    "http_scheme": "https",
    "catalog": "fizz",
    "schema": "buzz",
    "auth": ANY,
}


class StarburstClientTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_connection = Mock()
        self._mock_cursor = Mock()
        self._mock_connection.cursor.return_value = self._mock_cursor

    @patch("trino.dbapi.connect")
    def test_query_starburst_galaxy(self, mock_connect):
        query = "SELECT idx, value FROM table"  # noqa
        expected_data = [
            [
                "name_1",
                1,
            ],
            [
                "name_2",
                22.2,
            ],
        ]
        expected_description = [
            ["idx", "integer", None, None, None, None, None],
            ["value", "float", None, None, None, None, None],
        ]
        self._test_run_query(
            mock_connect,
            query,
            expected_data,
            expected_description,
            connection_type="starburst-galaxy",
        )

    @patch("trino.dbapi.connect")
    def test_query_starburst_enterprise(self, mock_connect):
        query = "SELECT idx, value FROM table"  # noqa
        expected_data = [
            [
                "name_1",
                1,
            ],
            [
                "name_2",
                22.2,
            ],
        ]
        expected_description = [
            ["idx", "integer", None, None, None, None, None],
            ["value", "float", None, None, None, None, None],
        ]
        self._test_run_query(
            mock_connect,
            query,
            expected_data,
            expected_description,
            connection_type="starburst-enterprise",
        )

    @patch("trino.dbapi.connect")
    def test_missing_credentials_raises_error(self, mock_connect):
        """Test that missing user or password raises an error"""
        operation_dict = {
            "trace_id": "1234",
            "skip_cache": True,
            "commands": [
                {"method": "cursor", "store": "_cursor"},
            ],
        }

        # Credentials without password
        credentials_no_password = {
            "host": "example.starburst.io",
            "port": "443",
            "http_scheme": "https",
            "catalog": "fizz",
            "schema": "buzz",
            "user": "foo",
        }

        response = self._agent.execute_operation(
            "starburst-galaxy",
            "run_query",
            operation_dict,
            {"connect_args": credentials_no_password},
        )

        self.assertIn("user", response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertIn("password", response.result.get(ATTRIBUTE_NAME_ERROR))
        mock_connect.assert_not_called()

        # Credentials without user
        credentials_no_user = {
            "host": "example.starburst.io",
            "port": "443",
            "http_scheme": "https",
            "catalog": "fizz",
            "schema": "buzz",
            "password": "bar",
        }

        response = self._agent.execute_operation(
            "starburst-galaxy",
            "run_query",
            operation_dict,
            {"connect_args": credentials_no_user},
        )

        self.assertIn("user", response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertIn("password", response.result.get(ATTRIBUTE_NAME_ERROR))
        mock_connect.assert_not_called()

    def _test_run_query(
        self,
        mock_connect: Mock,
        query: str,
        data: List,
        description: List,
        raise_exception: Optional[Exception] = None,
        expected_error_type: Optional[str] = None,
        connection_type: str = "starburst-galaxy",
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
            connection_type,
            "run_query",
            operation_dict,
            {
                "connect_args": _STARBURST_CREDENTIALS,
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

        mock_connect.assert_called_with(**_EXPECTED_STARBURST_CREDENTIALS)
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


class StarburstGalaxyCredentialShapeTests(TestCase):
    """Verify StarburstProxyClient __init__ accepts both DC-style and CTP-resolved credentials.

    DC path (today): DC plugin builds connect_args with all required fields and sends them to
    the agent. Port arrives as a string; user/password are converted to BasicAuthentication.

    CTP path (after Phase 2): flat credentials go through CTP, which converts port to int and
    hard-codes http_scheme before StarburstProxyClient is created.

    In both paths trino.dbapi.connect must receive the same effective arguments.
    """

    _HOST = "example.starburst.io"
    _PORT_STR = "443"
    _PORT_INT = 443
    _USER = "foo"
    _PASSWORD = "bar"

    def setUp(self) -> None:
        CtpRegistry.register("starburst-galaxy", STARBURST_GALAXY_DEFAULT_CTP)

    def tearDown(self) -> None:
        CtpRegistry._registry.pop("starburst-galaxy", None)

    def _dc_creds(self, **extra_connect_args):
        """Build DC-style credentials: connect_args with all required fields."""
        return {
            "connect_args": {
                "host": self._HOST,
                "port": self._PORT_STR,
                "http_scheme": "https",
                "user": self._USER,
                "password": self._PASSWORD,
                **extra_connect_args,
            }
        }

    def _ctp_creds(self, **flat_kwargs):
        """Build CTP-resolved credentials from flat input via the registry."""
        return CtpRegistry.resolve(
            "starburst-galaxy",
            {
                "host": self._HOST,
                "port": self._PORT_STR,
                "user": self._USER,
                "password": self._PASSWORD,
                **flat_kwargs,
            },
        )

    @patch("trino.dbapi.connect")
    def test_dc_path(self, mock_connect):
        """DC sends connect_args — port stays as string, user/password become BasicAuthentication."""
        mock_connect.return_value = Mock()
        StarburstProxyClient(credentials=self._dc_creds(), platform="test")

        kwargs = mock_connect.call_args.kwargs
        self.assertEqual(self._HOST, kwargs["host"])
        self.assertEqual(self._PORT_STR, kwargs["port"])  # string passes through as-is
        self.assertEqual("https", kwargs["http_scheme"])
        self.assertNotIn("user", kwargs)
        self.assertNotIn("password", kwargs)
        self.assertIn("auth", kwargs)

    @patch("trino.dbapi.connect")
    def test_ctp_path(self, mock_connect):
        """CTP resolves flat credentials — port is converted to int, http_scheme hard-coded."""
        mock_connect.return_value = Mock()
        StarburstProxyClient(credentials=self._ctp_creds(), platform="test")

        kwargs = mock_connect.call_args.kwargs
        self.assertEqual(self._HOST, kwargs["host"])
        self.assertEqual(self._PORT_INT, kwargs["port"])  # CTP converts to int
        self.assertEqual("https", kwargs["http_scheme"])
        self.assertNotIn("user", kwargs)
        self.assertNotIn("password", kwargs)
        self.assertIn("auth", kwargs)
