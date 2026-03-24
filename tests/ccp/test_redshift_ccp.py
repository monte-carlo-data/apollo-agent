# tests/ccp/test_redshift_ccp.py
from unittest import TestCase

from apollo.integrations.ccp.defaults.redshift import REDSHIFT_DEFAULT_CCP
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


class TestRedshiftCcp(TestCase):
    def test_not_registered(self):
        self.assertIsNone(CcpRegistry.get("redshift"))

    def test_resolve_flat_credentials(self):
        result = CcpPipeline().execute(
            REDSHIFT_DEFAULT_CCP,
            {
                "host": "cluster.abc123.us-east-1.redshift.amazonaws.com",
                "port": "5439",
                "db_name": "dev",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertEqual(
            "cluster.abc123.us-east-1.redshift.amazonaws.com", result["host"]
        )
        self.assertEqual(5439, result["port"])
        self.assertEqual("dev", result["dbname"])
        self.assertEqual("admin", result["user"])
        self.assertEqual("secret", result["password"])
        # DC hardcoded keepalives
        self.assertEqual(1, result["keepalives"])
        self.assertEqual(30, result["keepalives_idle"])
        self.assertEqual(10, result["keepalives_interval"])
        self.assertEqual(5, result["keepalives_count"])

    def test_port_coerced_to_int(self):
        result = CcpPipeline().execute(
            REDSHIFT_DEFAULT_CCP,
            {"host": "h", "port": "5439", "db_name": "d", "user": "u", "password": "p"},
        )
        self.assertIsInstance(result["port"], int)

    def test_default_user_awsuser(self):
        result = CcpPipeline().execute(
            REDSHIFT_DEFAULT_CCP,
            {"host": "h", "port": 5439, "db_name": "d", "password": "p"},
        )
        self.assertEqual("awsuser", result["user"])

    def test_default_port_5439(self):
        result = CcpPipeline().execute(
            REDSHIFT_DEFAULT_CCP,
            {"host": "h", "db_name": "d", "user": "u", "password": "p"},
        )
        self.assertEqual(5439, result["port"])

    def test_statement_timeout_from_query_timeout(self):
        result = CcpPipeline().execute(
            REDSHIFT_DEFAULT_CCP,
            {
                "host": "h",
                "db_name": "d",
                "user": "u",
                "password": "p",
                "query_timeout_in_seconds": 30,
            },
        )
        self.assertEqual("-c statement_timeout=30000", result["options"])

    def test_no_options_without_query_timeout(self):
        result = CcpPipeline().execute(
            REDSHIFT_DEFAULT_CCP,
            {"host": "h", "db_name": "d", "user": "u", "password": "p"},
        )
        self.assertNotIn("options", result)

    def test_dbname_fallback_variants(self):
        # Accepts db_name, dbname, or database
        for key in ("db_name", "dbname", "database"):
            result = CcpPipeline().execute(
                REDSHIFT_DEFAULT_CCP,
                {"host": "h", "port": 5439, key: "mydb", "user": "u", "password": "p"},
            )
            self.assertEqual("mydb", result["dbname"], f"failed for key={key}")
