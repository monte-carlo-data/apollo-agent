# tests/ccp/test_starburst_enterprise_ccp.py
import os
from unittest import TestCase

from apollo.integrations.ccp.defaults.starburst_enterprise import (
    STARBURST_ENTERPRISE_DEFAULT_CCP,
)
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry

_CA_PEM = "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----"


class TestStarburstEnterpriseCcp(TestCase):
    def test_not_registered(self):
        self.assertIsNone(CcpRegistry.get("starburst-enterprise"))

    def test_resolve_flat_no_ssl_options(self):
        result = CcpPipeline().execute(
            STARBURST_ENTERPRISE_DEFAULT_CCP,
            {
                "host": "ec2-3-88-168-18.compute-1.amazonaws.com",
                "port": "8443",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertEqual("ec2-3-88-168-18.compute-1.amazonaws.com", result["host"])
        self.assertEqual(8443, result["port"])
        self.assertEqual("admin", result["user"])
        self.assertEqual("secret", result["password"])
        self.assertEqual("https", result["http_scheme"])
        self.assertNotIn("verify", result)
        self.assertNotIn("ssl_options", result)

    def test_resolve_flat_ssl_disabled(self):
        result = CcpPipeline().execute(
            STARBURST_ENTERPRISE_DEFAULT_CCP,
            {
                "host": "ec2-3-88-168-18.compute-1.amazonaws.com",
                "port": "8443",
                "user": "admin",
                "password": "secret",
                "ssl_options": {"disabled": True},
            },
        )
        self.assertEqual("https", result["http_scheme"])
        self.assertIs(False, result["verify"])
        self.assertNotIn("ssl_options", result)

    def test_resolve_flat_ssl_ca_data(self):
        result = CcpPipeline().execute(
            STARBURST_ENTERPRISE_DEFAULT_CCP,
            {
                "host": "ec2-3-88-168-18.compute-1.amazonaws.com",
                "port": "8443",
                "user": "admin",
                "password": "secret",
                "ssl_options": {"ca_data": _CA_PEM},
            },
        )
        self.assertEqual("https", result["http_scheme"])
        self.assertNotIn("ssl_options", result)
        cert_path = result["verify"]
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
        path1 = CcpPipeline().execute(STARBURST_ENTERPRISE_DEFAULT_CCP, creds)["verify"]
        path2 = CcpPipeline().execute(STARBURST_ENTERPRISE_DEFAULT_CCP, creds)["verify"]
        self.assertEqual(path1, path2)
        os.unlink(path1)
