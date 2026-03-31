# tests/ctp/test_redshift_ctp.py
from unittest import TestCase

from apollo.integrations.ctp.defaults.redshift import REDSHIFT_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


class TestRedshiftCtp(TestCase):
    def test_not_registered(self):
        self.assertIsNone(CtpRegistry.get("redshift"))

    def test_resolve_flat_credentials(self):
        result = CtpPipeline().execute(
            REDSHIFT_DEFAULT_CTP,
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
        result = CtpPipeline().execute(
            REDSHIFT_DEFAULT_CTP,
            {"host": "h", "port": "5439", "db_name": "d", "user": "u", "password": "p"},
        )
        self.assertIsInstance(result["port"], int)

    def test_default_user_awsuser(self):
        result = CtpPipeline().execute(
            REDSHIFT_DEFAULT_CTP,
            {"host": "h", "port": 5439, "db_name": "d", "password": "p"},
        )
        self.assertEqual("awsuser", result["user"])

    def test_default_port_5439(self):
        result = CtpPipeline().execute(
            REDSHIFT_DEFAULT_CTP,
            {"host": "h", "db_name": "d", "user": "u", "password": "p"},
        )
        self.assertEqual(5439, result["port"])

    def test_statement_timeout_from_query_timeout(self):
        result = CtpPipeline().execute(
            REDSHIFT_DEFAULT_CTP,
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
        result = CtpPipeline().execute(
            REDSHIFT_DEFAULT_CTP,
            {"host": "h", "db_name": "d", "user": "u", "password": "p"},
        )
        self.assertNotIn("options", result)

    def test_dbname_fallback_variants(self):
        # Accepts db_name, dbname, or database
        for key in ("db_name", "dbname", "database"):
            result = CtpPipeline().execute(
                REDSHIFT_DEFAULT_CTP,
                {"host": "h", "port": 5439, key: "mydb", "user": "u", "password": "p"},
            )
            self.assertEqual("mydb", result["dbname"], f"failed for key={key}")
