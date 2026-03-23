# tests/ccp/test_starburst_galaxy_ccp.py
from unittest import TestCase

from apollo.integrations.ccp.registry import CcpRegistry


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

    def test_resolve_flat_no_ssl_options(self):
        result = CcpRegistry.resolve(
            "starburst-galaxy",
            {
                "host": "example.trino.galaxy.starburst.io",
                "port": "443",
                "user": "service@example.galaxy.starburst.io",
                "password": "secret",
            },
        )
        ca = result["connect_args"]
        self.assertNotIn("verify", ca)
        self.assertNotIn("ssl_options", ca)

    def test_resolve_flat_ssl_disabled(self):
        result = CcpRegistry.resolve(
            "starburst-galaxy",
            {
                "host": "example.trino.galaxy.starburst.io",
                "port": "443",
                "user": "service@example.galaxy.starburst.io",
                "password": "secret",
                "ssl_options": {"disabled": True},
            },
        )
        ca = result["connect_args"]
        self.assertIs(False, ca["verify"])
        self.assertNotIn("ssl_options", ca)

    def test_resolve_flat_ssl_ca_data(self):
        result = CcpRegistry.resolve(
            "starburst-galaxy",
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
        ca = result["connect_args"]
        self.assertIn("verify", ca)
        self.assertIsInstance(ca["verify"], str)
        self.assertTrue(ca["verify"].endswith("_ssl_ca.pem"))
        self.assertNotIn("ssl_options", ca)

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
