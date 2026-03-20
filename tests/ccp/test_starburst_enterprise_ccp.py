# tests/ccp/test_starburst_enterprise_ccp.py
from unittest import TestCase

from apollo.integrations.ccp.registry import CcpRegistry


class TestStarburstEnterpriseCcp(TestCase):
    def test_starburst_enterprise_registered(self):
        config = CcpRegistry.get("starburst-enterprise")
        self.assertIsNotNone(config)
        self.assertEqual("starburst-enterprise-default", config.name)

    def test_resolve_flat_starburst_enterprise_no_ssl(self):
        result = CcpRegistry.resolve(
            "starburst-enterprise",
            {
                "host": "ec2-3-88-168-18.compute-1.amazonaws.com",
                "port": "8443",
                "user": "admin",
                "password": "secret",
                "ssl_options": {"disabled": True},
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("ec2-3-88-168-18.compute-1.amazonaws.com", ca["host"])
        self.assertEqual(8443, ca["port"])
        self.assertEqual("admin", ca["user"])
        self.assertEqual("secret", ca["password"])
        self.assertEqual("https", ca["http_scheme"])
        self.assertEqual({"disabled": True}, ca["ssl_options"])

    def test_resolve_flat_starburst_enterprise_ca_data(self):
        result = CcpRegistry.resolve(
            "starburst-enterprise",
            {
                "host": "ec2-3-88-168-18.compute-1.amazonaws.com",
                "port": "8443",
                "user": "admin",
                "password": "secret",
                "ssl_options": {
                    "ca_data": "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----"
                },
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual(8443, ca["port"])
        self.assertEqual("https", ca["http_scheme"])
        self.assertIn("ca_data", ca["ssl_options"])

    def test_resolve_flat_starburst_enterprise_no_ssl_options(self):
        result = CcpRegistry.resolve(
            "starburst-enterprise",
            {
                "host": "ec2-3-88-168-18.compute-1.amazonaws.com",
                "port": "8443",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("https", ca["http_scheme"])
        self.assertNotIn("ssl_options", ca)

    def test_resolve_legacy_starburst_enterprise_credentials_unchanged(self):
        legacy = {
            "connect_args": {
                "host": "h",
                "port": 8443,
                "user": "u",
                "password": "p",
                "http_scheme": "https",
                "ssl_options": {"disabled": True},
            }
        }
        self.assertEqual(legacy, CcpRegistry.resolve("starburst-enterprise", legacy))
