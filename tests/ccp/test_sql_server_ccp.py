# tests/ccp/test_sql_server_ccp.py
#
# The proxy clients currently expect connect_args to be a pre-built ODBC string
# (constructed by the DC). CCP produces a dict of ODBC key-value pairs that Phase 2
# will serialize. These configs are not registered; tests use CcpPipeline().execute()
# directly.
from unittest import TestCase

from apollo.integrations.ccp.defaults.sql_server import (
    AZURE_DEDICATED_SQL_POOL_DEFAULT_CCP,
    AZURE_SQL_DATABASE_DEFAULT_CCP,
    SQL_SERVER_DEFAULT_CCP,
)
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry

_ALL_CONFIGS = [
    ("sql-server", SQL_SERVER_DEFAULT_CCP),
    ("azure-sql-database", AZURE_SQL_DATABASE_DEFAULT_CCP),
    ("azure-dedicated-sql-pool", AZURE_DEDICATED_SQL_POOL_DEFAULT_CCP),
]


def _resolve(config, credentials: dict) -> dict:
    return CcpPipeline().execute(config, credentials)


class TestSqlServerCcp(TestCase):
    def test_sql_server_variants_not_registered(self):
        for connection_type, _ in _ALL_CONFIGS:
            with self.subTest(connection_type=connection_type):
                self.assertIsNone(CcpRegistry.get(connection_type))

    # ── Basic connection fields ────────────────────────────────────────

    def test_sql_server_basic_connection(self):
        args = _resolve(
            SQL_SERVER_DEFAULT_CCP,
            {
                "host": "db.example.com",
                "port": 1433,
                "user": "alice",
                "password": "secret",
            },
        )
        self.assertEqual("{ODBC Driver 17 for SQL Server}", args["DRIVER"])
        self.assertEqual("tcp:db.example.com,1433", args["SERVER"])
        self.assertEqual("alice", args["UID"])
        self.assertEqual("secret", args["PWD"])
        self.assertEqual("Yes", args["MARS_Connection"])

    def test_port_defaults_to_1433(self):
        args = _resolve(
            SQL_SERVER_DEFAULT_CCP,
            {"host": "db.example.com", "user": "u", "password": "p"},
        )
        self.assertEqual("tcp:db.example.com,1433", args["SERVER"])

    def test_username_field_alias(self):
        args = _resolve(
            SQL_SERVER_DEFAULT_CCP,
            {"host": "h", "port": 1433, "username": "bob", "password": "p"},
        )
        self.assertEqual("bob", args["UID"])

    def test_sql_server_no_database_field(self):
        args = _resolve(
            SQL_SERVER_DEFAULT_CCP,
            {"host": "h", "port": 1433, "user": "u", "password": "p"},
        )
        self.assertNotIn("DATABASE", args)

    # ── Azure variants — DATABASE field ───────────────────────────────

    def test_azure_sql_database_includes_database(self):
        args = _resolve(
            AZURE_SQL_DATABASE_DEFAULT_CCP,
            {
                "host": "myserver.database.windows.net",
                "port": 1433,
                "user": "u",
                "password": "p",
                "db_name": "mydb",
            },
        )
        self.assertEqual("mydb", args["DATABASE"])
        self.assertEqual("tcp:myserver.database.windows.net,1433", args["SERVER"])

    def test_azure_dedicated_sql_pool_includes_database(self):
        args = _resolve(
            AZURE_DEDICATED_SQL_POOL_DEFAULT_CCP,
            {
                "host": "mypool.sql.azuresynapse.net",
                "port": 1433,
                "user": "u",
                "password": "p",
                "database": "mypool_db",
            },
        )
        self.assertEqual("mypool_db", args["DATABASE"])

    def test_azure_database_field_alias(self):
        # db_name takes precedence over database when both present
        args = _resolve(
            AZURE_SQL_DATABASE_DEFAULT_CCP,
            {
                "host": "h",
                "port": 1433,
                "user": "u",
                "password": "p",
                "db_name": "primary",
                "database": "fallback",
            },
        )
        self.assertEqual("primary", args["DATABASE"])

    # ── Azure variants share base fields ─────────────────────────────

    def test_azure_variants_share_driver_and_mars(self):
        for _, config in [
            ("azure-sql-database", AZURE_SQL_DATABASE_DEFAULT_CCP),
            ("azure-dedicated-sql-pool", AZURE_DEDICATED_SQL_POOL_DEFAULT_CCP),
        ]:
            with self.subTest(config=config.name):
                args = _resolve(
                    config,
                    {
                        "host": "h",
                        "port": 1433,
                        "user": "u",
                        "password": "p",
                        "db_name": "d",
                    },
                )
                self.assertEqual("{ODBC Driver 17 for SQL Server}", args["DRIVER"])
                self.assertEqual("Yes", args["MARS_Connection"])
