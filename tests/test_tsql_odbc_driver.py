from unittest import TestCase
from unittest.mock import Mock, patch

from apollo.integrations.db import tsql_base_db_proxy_client
from apollo.integrations.db.azure_database_proxy_client import (
    AzureDatabaseProxyClient,
)
from apollo.integrations.db.fabric_proxy_client import MsFabricProxyClient
from apollo.integrations.db.sql_server_proxy_client import SqlServerProxyClient
from apollo.integrations.db.tsql_base_db_proxy_client import normalize_odbc_driver

_DRIVER_17 = "ODBC Driver 17 for SQL Server"
_DRIVER_18 = "ODBC Driver 18 for SQL Server"
_LEGACY = f"DRIVER={{{_DRIVER_17}}};SERVER=tcp:db.example.com,1433;UID=user;PWD=pass"


class NormalizeOdbcDriverTests(TestCase):
    def setUp(self) -> None:
        tsql_base_db_proxy_client._installed_odbc_drivers.cache_clear()
        self.addCleanup(tsql_base_db_proxy_client._installed_odbc_drivers.cache_clear)

    def _with_drivers(self, *drivers: str) -> None:
        drivers_patch = patch("pyodbc.drivers", return_value=list(drivers))
        drivers_patch.start()
        self.addCleanup(drivers_patch.stop)

    def test_unchanged_when_driver_17_installed(self):
        self._with_drivers(_DRIVER_17, _DRIVER_18)
        self.assertEqual(_LEGACY, normalize_odbc_driver(_LEGACY))

    def test_switches_to_driver_18_and_keeps_driver_17_encryption_default(self):
        self._with_drivers(_DRIVER_18)
        self.assertEqual(
            f"DRIVER={{{_DRIVER_18}}};SERVER=tcp:db.example.com,1433;UID=user;PWD=pass;Encrypt=no",
            normalize_odbc_driver(_LEGACY),
        )

    def test_keeps_explicit_encrypt(self):
        self._with_drivers(_DRIVER_18)
        self.assertEqual(
            f"DRIVER={{{_DRIVER_18}}};SERVER=tcp:db.example.com;encrypt=yes;TrustServerCertificate=no",
            normalize_odbc_driver(
                f"Driver={{{_DRIVER_17}}};SERVER=tcp:db.example.com;encrypt=yes;TrustServerCertificate=no"
            ),
        )

    def test_preserves_braced_values(self):
        self._with_drivers(_DRIVER_18)
        self.assertEqual(
            f"DRIVER={{{_DRIVER_18}}};PWD={{p;a}}}}ss}};UID=user;Encrypt=no",
            normalize_odbc_driver(
                f"DRIVER={{{_DRIVER_17}}};PWD={{p;a}}}}ss}};UID=user"
            ),
        )

    def test_unchanged_when_no_sql_server_driver_installed(self):
        self._with_drivers()
        self.assertEqual(_LEGACY, normalize_odbc_driver(_LEGACY))

    def test_other_drivers_untouched(self):
        self._with_drivers(_DRIVER_18)
        for connection_string in (
            f"DRIVER={{{_DRIVER_18}}};SERVER=tcp:db.example.com",
            "DRIVER={FreeTDS};SERVER=db.example.com",
            "SERVER=db.example.com;UID=user",
        ):
            self.assertEqual(
                connection_string, normalize_odbc_driver(connection_string)
            )


class ProxyClientsNormalizeDriverTests(TestCase):
    """Each T-SQL proxy client connects with the normalized connection string."""

    def setUp(self) -> None:
        tsql_base_db_proxy_client._installed_odbc_drivers.cache_clear()
        self.addCleanup(tsql_base_db_proxy_client._installed_odbc_drivers.cache_clear)
        drivers_patch = patch("pyodbc.drivers", return_value=[_DRIVER_18])
        drivers_patch.start()
        self.addCleanup(drivers_patch.stop)

    def _connected_string(self, client_class, connect_args) -> str:
        with patch("pyodbc.connect", return_value=Mock()) as mock_connect:
            client_class(credentials={"connect_args": connect_args})
        return mock_connect.call_args.args[0]

    def test_sql_server_legacy_string(self):
        self.assertTrue(
            self._connected_string(SqlServerProxyClient, _LEGACY).startswith(
                f"DRIVER={{{_DRIVER_18}}};"
            )
        )

    def test_azure_database_legacy_string(self):
        self.assertTrue(
            self._connected_string(AzureDatabaseProxyClient, _LEGACY).endswith(
                ";Encrypt=no"
            )
        )

    def test_fabric_dict(self):
        connected = self._connected_string(
            MsFabricProxyClient,
            {"DRIVER": f"{{{_DRIVER_17}}}", "SERVER": "fabric.example.com"},
        )
        self.assertEqual(
            f"DRIVER={{{_DRIVER_18}}};SERVER=fabric.example.com;Encrypt=no", connected
        )
