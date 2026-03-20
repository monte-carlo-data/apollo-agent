# tests/ccp/test_starburst_enterprise_ccp.py
import os
from unittest import TestCase

from apollo.integrations.ccp.registry import CcpRegistry

_CA_PEM = "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----"


class TestStarburstEnterpriseCcp(TestCase):
    def test_starburst_enterprise_registered(self):
        config = CcpRegistry.get("starburst-enterprise")
        self.assertIsNotNone(config)
        self.assertEqual("starburst-enterprise-default", config.name)

    def test_resolve_flat_no_ssl_options(self):
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
        self.assertEqual("ec2-3-88-168-18.compute-1.amazonaws.com", ca["host"])
        self.assertEqual(8443, ca["port"])
        self.assertEqual("admin", ca["user"])
        self.assertEqual("secret", ca["password"])
        self.assertEqual("https", ca["http_scheme"])
        self.assertNotIn("verify", ca)
        self.assertNotIn("ssl_options", ca)

    def test_resolve_flat_ssl_disabled(self):
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
        self.assertEqual("https", ca["http_scheme"])
        self.assertIs(False, ca["verify"])
        self.assertNotIn("ssl_options", ca)

    def test_resolve_flat_ssl_ca_data(self):
        result = CcpRegistry.resolve(
            "starburst-enterprise",
            {
                "host": "ec2-3-88-168-18.compute-1.amazonaws.com",
                "port": "8443",
                "user": "admin",
                "password": "secret",
                "ssl_options": {"ca_data": _CA_PEM},
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("https", ca["http_scheme"])
        self.assertNotIn("ssl_options", ca)
        cert_path = ca["verify"]
        self.assertIsInstance(cert_path, str)
        self.assertTrue(os.path.exists(cert_path))
        with open(cert_path) as f:
            self.assertEqual(_CA_PEM, f.read())
        os.unlink(cert_path)

    def test_resolve_flat_ssl_ca_data_is_deterministic(self):
        """Same CA content always resolves to the same file path."""
        creds = {
            "host": "h",
            "port": "8443",
            "user": "u",
            "password": "p",
            "ssl_options": {"ca_data": _CA_PEM},
        }
        path1 = CcpRegistry.resolve("starburst-enterprise", creds)["connect_args"][
            "verify"
        ]
        path2 = CcpRegistry.resolve("starburst-enterprise", creds)["connect_args"][
            "verify"
        ]
        self.assertEqual(path1, path2)
        os.unlink(path1)

    def test_resolve_legacy_credentials_unchanged(self):
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
