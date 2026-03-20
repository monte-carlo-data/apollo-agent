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


class TestStarburstGalaxyCcp(TestCase):
    def test_starburst_galaxy_registered(self):
        config = CcpRegistry.get("starburst-galaxy")
        self.assertIsNotNone(config)
        self.assertEqual("starburst-galaxy-default", config.name)

    def test_resolve_flat_starburst_galaxy_credentials(self):
        result = CcpRegistry.resolve(
            "starburst-galaxy",
            {
                "host": "example.trino.galaxy.starburst.io",
                "port": "443",
                "user": "service@example.galaxy.starburst.io",
                "password": "secret",
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("example.trino.galaxy.starburst.io", ca["host"])
        self.assertEqual(443, ca["port"])
        self.assertEqual("service@example.galaxy.starburst.io", ca["user"])
        self.assertEqual("secret", ca["password"])
        self.assertEqual("https", ca["http_scheme"])

    def test_resolve_legacy_starburst_galaxy_credentials_unchanged(self):
        legacy = {
            "connect_args": {
                "host": "h",
                "port": 443,
                "user": "u",
                "password": "p",
                "http_scheme": "https",
            }
        }
        self.assertEqual(legacy, CcpRegistry.resolve("starburst-galaxy", legacy))
