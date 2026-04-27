# tests/ctp/test_postgres_ctp.py
import os
from unittest import TestCase

from apollo.integrations.ctp.defaults.postgres import POSTGRES_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


class TestPostgresCtp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CtpRegistry.get("postgres"))

    def test_resolve_flat_postgres_credentials_applies_ctp(self):
        result = CtpPipeline().execute(
            POSTGRES_DEFAULT_CTP,
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
        result = CtpPipeline().execute(
            POSTGRES_DEFAULT_CTP,
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
        result = CtpPipeline().execute(
            POSTGRES_DEFAULT_CTP,
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
