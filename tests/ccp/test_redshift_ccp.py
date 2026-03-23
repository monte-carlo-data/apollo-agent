# tests/ccp/test_redshift_ccp.py
from unittest import TestCase

from apollo.integrations.ccp.registry import CcpRegistry


class TestRedshiftCcp(TestCase):
    def test_redshift_registered(self):
        config = CcpRegistry.get("redshift")
        self.assertIsNotNone(config)
        self.assertEqual("redshift-default", config.name)

    def test_resolve_flat_credentials(self):
        result = CcpRegistry.resolve(
            "redshift",
            {
                "host": "cluster.abc123.us-east-1.redshift.amazonaws.com",
                "port": "5439",
                "db_name": "dev",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("cluster.abc123.us-east-1.redshift.amazonaws.com", ca["host"])
        self.assertEqual(5439, ca["port"])
        self.assertEqual("dev", ca["dbname"])
        self.assertEqual("admin", ca["user"])
        self.assertEqual("secret", ca["password"])
        # DC hardcoded keepalives
        self.assertEqual(1, ca["keepalives"])
        self.assertEqual(30, ca["keepalives_idle"])
        self.assertEqual(10, ca["keepalives_interval"])
        self.assertEqual(5, ca["keepalives_count"])

    def test_port_coerced_to_int(self):
        result = CcpRegistry.resolve(
            "redshift",
            {"host": "h", "port": "5439", "db_name": "d", "user": "u", "password": "p"},
        )
        self.assertIsInstance(result["connect_args"]["port"], int)

    def test_default_user_awsuser(self):
        result = CcpRegistry.resolve(
            "redshift",
            {"host": "h", "port": 5439, "db_name": "d", "password": "p"},
        )
        self.assertEqual("awsuser", result["connect_args"]["user"])

    def test_default_port_5439(self):
        result = CcpRegistry.resolve(
            "redshift",
            {"host": "h", "db_name": "d", "user": "u", "password": "p"},
        )
        self.assertEqual(5439, result["connect_args"]["port"])

    def test_statement_timeout_from_query_timeout(self):
        result = CcpRegistry.resolve(
            "redshift",
            {
                "host": "h",
                "db_name": "d",
                "user": "u",
                "password": "p",
                "query_timeout_in_seconds": 30,
            },
        )
        self.assertEqual(
            "-c statement_timeout=30000", result["connect_args"]["options"]
        )

    def test_no_options_without_query_timeout(self):
        result = CcpRegistry.resolve(
            "redshift",
            {"host": "h", "db_name": "d", "user": "u", "password": "p"},
        )
        self.assertNotIn("options", result["connect_args"])

    def test_dbname_fallback_variants(self):
        # Accepts db_name, dbname, or database
        for key in ("db_name", "dbname", "database"):
            result = CcpRegistry.resolve(
                "redshift",
                {"host": "h", "port": 5439, key: "mydb", "user": "u", "password": "p"},
            )
            self.assertEqual(
                "mydb", result["connect_args"]["dbname"], f"failed for key={key}"
            )

    def test_resolve_legacy_credentials_unchanged(self):
        legacy = {"connect_args": {"host": "h", "port": 5439, "dbname": "d"}}
        self.assertEqual(legacy, CcpRegistry.resolve("redshift", legacy))
