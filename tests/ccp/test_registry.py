# tests/ccp/test_registry.py
import os
from unittest import TestCase

from apollo.integrations.ccp.registry import CcpRegistry


class TestCcpRegistry(TestCase):
    def test_unknown_type_returns_none(self):
        self.assertIsNone(CcpRegistry.get("not_a_real_type"))

    def test_resolve_unknown_type_returns_credentials_unchanged(self):
        creds = {"host": "db.example.com"}
        self.assertEqual(creds, CcpRegistry.resolve("unknown_type", creds))

    def test_resolve_legacy_credentials_returned_unchanged(self):
        # import to trigger registration
        import apollo.integrations.ccp.defaults.postgres  # noqa
        legacy = {"connect_args": {"host": "db.example.com", "dbname": "mydb"}}
        self.assertEqual(legacy, CcpRegistry.resolve("postgres", legacy))

    def test_postgres_registered(self):
        import apollo.integrations.ccp.defaults.postgres  # noqa
        config = CcpRegistry.get("postgres")
        self.assertIsNotNone(config)
        self.assertEqual("postgres-default", config.name)

    def test_resolve_flat_postgres_credentials_applies_ccp(self):
        import apollo.integrations.ccp.defaults.postgres  # noqa
        result = CcpRegistry.resolve("postgres", {
            "host": "db.example.com",
            "port": 5432,
            "database": "mydb",
            "user": "admin",
            "password": "secret",
        })
        self.assertIn("connect_args", result)
        self.assertEqual("db.example.com", result["connect_args"]["host"])
        self.assertEqual("mydb", result["connect_args"]["dbname"])
        self.assertEqual("require", result["connect_args"]["sslmode"])
        self.assertNotIn("sslrootcert", result["connect_args"])

    def test_resolve_flat_postgres_with_ssl_ca_pem(self):
        import apollo.integrations.ccp.defaults.postgres  # noqa
        result = CcpRegistry.resolve("postgres", {
            "host": "db.example.com",
            "port": 5432,
            "database": "mydb",
            "user": "admin",
            "password": "secret",
            "ssl_ca_pem": "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----",
        })
        path = result["connect_args"]["sslrootcert"]
        self.assertTrue(os.path.exists(path))
        os.unlink(path)
