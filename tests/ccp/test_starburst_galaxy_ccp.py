# tests/ccp/test_starburst_galaxy_ccp.py
from unittest import TestCase

from apollo.integrations.ccp.defaults.starburst_galaxy import (
    STARBURST_GALAXY_DEFAULT_CCP,
)
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


class TestStarburstGalaxyCcp(TestCase):
    def test_not_registered(self):
        self.assertIsNone(CcpRegistry.get("starburst-galaxy"))

    def test_resolve_flat_starburst_galaxy_credentials(self):
        result = CcpPipeline().execute(
            STARBURST_GALAXY_DEFAULT_CCP,
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
        result = CcpPipeline().execute(
            STARBURST_GALAXY_DEFAULT_CCP,
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
        result = CcpPipeline().execute(
            STARBURST_GALAXY_DEFAULT_CCP,
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
        result = CcpPipeline().execute(
            STARBURST_GALAXY_DEFAULT_CCP,
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
