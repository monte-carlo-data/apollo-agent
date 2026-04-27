# tests/ctp/test_sql_server_ctp.py
from unittest import TestCase

from apollo.integrations.ctp.defaults.sql_server import (
    AZURE_DEDICATED_SQL_POOL_DEFAULT_CTP,
    AZURE_SQL_DATABASE_DEFAULT_CTP,
    SQL_SERVER_DEFAULT_CTP,
)
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry

_ALL_CONFIGS = [
    ("sql-server", SQL_SERVER_DEFAULT_CTP),
    ("azure-sql-database", AZURE_SQL_DATABASE_DEFAULT_CTP),
    ("azure-dedicated-sql-pool", AZURE_DEDICATED_SQL_POOL_DEFAULT_CTP),
]


def _resolve(config, credentials: dict) -> dict:
    return CtpPipeline().execute(config, credentials)


class TestSqlServerCtp(TestCase):
    def test_sql_server_variants_registered(self):
        for connection_type, _ in _ALL_CONFIGS:
            with self.subTest(connection_type=connection_type):
                self.assertIsNotNone(CtpRegistry.get(connection_type))

    # ── Basic connection fields ────────────────────────────────────────

    def test_sql_server_basic_connection(self):
        args = _resolve(
            SQL_SERVER_DEFAULT_CTP,
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
            SQL_SERVER_DEFAULT_CTP,
            {"host": "db.example.com", "user": "u", "password": "p"},
        )
        self.assertEqual("tcp:db.example.com,1433", args["SERVER"])

    def test_username_field_alias(self):
        args = _resolve(
            SQL_SERVER_DEFAULT_CTP,
            {"host": "h", "port": 1433, "username": "bob", "password": "p"},
        )
        self.assertEqual("bob", args["UID"])

    def test_sql_server_no_database_field(self):
        args = _resolve(
            SQL_SERVER_DEFAULT_CTP,
            {"host": "h", "port": 1433, "user": "u", "password": "p"},
        )
        self.assertNotIn("DATABASE", args)

    # ── Azure variants — DATABASE field ───────────────────────────────

    def test_azure_sql_database_includes_database(self):
        args = _resolve(
            AZURE_SQL_DATABASE_DEFAULT_CTP,
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
            AZURE_DEDICATED_SQL_POOL_DEFAULT_CTP,
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
            AZURE_SQL_DATABASE_DEFAULT_CTP,
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
            ("azure-sql-database", AZURE_SQL_DATABASE_DEFAULT_CTP),
            ("azure-dedicated-sql-pool", AZURE_DEDICATED_SQL_POOL_DEFAULT_CTP),
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
