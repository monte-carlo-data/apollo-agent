# tests/ccp/test_registry.py
import os
from unittest import TestCase

from apollo.integrations.ccp.errors import CcpPipelineError
from apollo.integrations.ccp.registry import CcpRegistry


class TestCcpRegistry(TestCase):
    def test_unknown_type_returns_none(self):
        self.assertIsNone(CcpRegistry.get("not_a_real_type"))

    def test_resolve_unknown_type_raises(self):
        with self.assertRaises(CcpPipelineError):
            CcpRegistry.resolve("unknown_type", {"host": "db.example.com"})

    def test_resolve_legacy_credentials_returned_unchanged(self):
        legacy = {"connect_args": {"host": "db.example.com", "dbname": "mydb"}}
        self.assertEqual(legacy, CcpRegistry.resolve("postgres", legacy))

    def test_postgres_registered(self):
        config = CcpRegistry.get("postgres")
        self.assertIsNotNone(config)
        self.assertEqual("postgres-default", config.name)

    def test_resolve_flat_postgres_credentials_applies_ccp(self):
        result = CcpRegistry.resolve(
            "postgres",
            {
                "host": "db.example.com",
                "port": 5432,
                "database": "mydb",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertIn("connect_args", result)
        self.assertEqual("db.example.com", result["connect_args"]["host"])
        self.assertEqual("mydb", result["connect_args"]["dbname"])
        self.assertNotIn("sslmode", result["connect_args"])
        self.assertNotIn("sslrootcert", result["connect_args"])

    def test_resolve_flat_postgres_with_explicit_ssl_mode(self):
        result = CcpRegistry.resolve(
            "postgres",
            {
                "host": "db.example.com",
                "port": 5432,
                "database": "mydb",
                "user": "admin",
                "password": "secret",
                "ssl_mode": "verify-full",
            },
        )
        self.assertEqual("verify-full", result["connect_args"]["sslmode"])

    def test_resolve_flat_postgres_with_ssl_ca_pem(self):
        result = CcpRegistry.resolve(
            "postgres",
            {
                "host": "db.example.com",
                "port": 5432,
                "database": "mydb",
                "user": "admin",
                "password": "secret",
                "ssl_ca_pem": "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----",
            },
        )
        path = result["connect_args"]["sslrootcert"]
        self.assertTrue(os.path.exists(path))
        self.assertEqual("require", result["connect_args"]["sslmode"])
        os.unlink(path)
