# tests/ccp/test_postgres_ccp.py
import os
from unittest import TestCase

from apollo.integrations.ccp.defaults.postgres import POSTGRES_DEFAULT_CCP
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


class TestPostgresCcp(TestCase):
    def test_not_registered(self):
        self.assertIsNone(CcpRegistry.get("postgres"))

    def test_resolve_flat_postgres_credentials_applies_ccp(self):
        result = CcpPipeline().execute(
            POSTGRES_DEFAULT_CCP,
            {
                "host": "db.example.com",
                "port": 5432,
                "database": "mydb",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertEqual("db.example.com", result["host"])
        self.assertEqual("mydb", result["dbname"])
        self.assertNotIn("sslmode", result)
        self.assertNotIn("sslrootcert", result)

    def test_resolve_flat_postgres_with_explicit_ssl_mode(self):
        result = CcpPipeline().execute(
            POSTGRES_DEFAULT_CCP,
            {
                "host": "db.example.com",
                "port": 5432,
                "database": "mydb",
                "user": "admin",
                "password": "secret",
                "ssl_mode": "verify-full",
            },
        )
        self.assertEqual("verify-full", result["sslmode"])

    def test_resolve_flat_postgres_with_ssl_ca_data(self):
        result = CcpPipeline().execute(
            POSTGRES_DEFAULT_CCP,
            {
                "host": "db.example.com",
                "port": 5432,
                "database": "mydb",
                "user": "admin",
                "password": "secret",
                "ssl_options": {
                    "ca_data": "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----"
                },
            },
        )
        path = result["sslrootcert"]
        self.assertTrue(os.path.exists(path))
        self.assertEqual("require", result["sslmode"])
        os.unlink(path)
