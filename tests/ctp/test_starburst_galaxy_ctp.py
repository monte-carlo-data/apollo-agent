# tests/ctp/test_starburst_galaxy_ctp.py
from unittest import TestCase

from apollo.integrations.ctp.defaults.starburst_galaxy import (
    STARBURST_GALAXY_DEFAULT_CTP,
)
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


class TestStarburstGalaxyCtp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CtpRegistry.get("starburst-galaxy"))

    def test_resolve_flat_starburst_galaxy_credentials(self):
        result = CtpPipeline().execute(
            STARBURST_GALAXY_DEFAULT_CTP,
            {
                "host": "example.trino.galaxy.starburst.io",
                "port": "443",
                "user": "service@example.galaxy.starburst.io",
                "password": "secret",
            },
        )
        self.assertEqual("example.trino.galaxy.starburst.io", result["host"])
        self.assertEqual(443, result["port"])
        self.assertEqual("service@example.galaxy.starburst.io", result["user"])
        self.assertEqual("secret", result["password"])
        self.assertEqual("https", result["http_scheme"])

    def test_resolve_flat_no_ssl_options(self):
        result = CtpPipeline().execute(
            STARBURST_GALAXY_DEFAULT_CTP,
            {
                "host": "example.trino.galaxy.starburst.io",
                "port": "443",
                "user": "service@example.galaxy.starburst.io",
                "password": "secret",
            },
        )
        self.assertNotIn("verify", result)
        self.assertNotIn("ssl_options", result)

    def test_resolve_flat_ssl_disabled(self):
        result = CtpPipeline().execute(
            STARBURST_GALAXY_DEFAULT_CTP,
            {
                "host": "example.trino.galaxy.starburst.io",
                "port": "443",
                "user": "service@example.galaxy.starburst.io",
                "password": "secret",
                "ssl_options": {"disabled": True},
            },
        )
        self.assertIs(False, result["verify"])
        self.assertNotIn("ssl_options", result)

    def test_resolve_flat_ssl_ca_data(self):
        result = CtpPipeline().execute(
            STARBURST_GALAXY_DEFAULT_CTP,
            {
                "host": "example.trino.galaxy.starburst.io",
                "port": "443",
                "user": "service@example.galaxy.starburst.io",
                "password": "secret",
                "ssl_options": {
                    "ca_data": "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n"
                },
            },
        )
        self.assertIn("verify", result)
        self.assertIsInstance(result["verify"], str)
        self.assertTrue(result["verify"].endswith("_ssl_ca.pem"))
        self.assertNotIn("ssl_options", result)
