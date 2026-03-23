# tests/ccp/test_salesforce_crm_ccp.py
from unittest import TestCase

from apollo.integrations.ccp.registry import CcpRegistry


class TestSalesforceCrmCcp(TestCase):
    def test_salesforce_crm_registered(self):
        config = CcpRegistry.get("salesforce-crm")
        self.assertIsNotNone(config)
        self.assertEqual("salesforce-crm-default", config.name)

    def test_resolve_token_auth(self):
        result = CcpRegistry.resolve(
            "salesforce-crm",
            {
                "user": "admin@example.com",
                "password": "secret",
                "security_token": "ABC123",
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("admin@example.com", ca["username"])
        self.assertEqual("secret", ca["password"])
        self.assertEqual("ABC123", ca["security_token"])
        self.assertNotIn("consumer_key", ca)
        self.assertNotIn("domain", ca)

    def test_user_mapped_to_username(self):
        result = CcpRegistry.resolve(
            "salesforce-crm",
            {"user": "admin@example.com", "password": "p", "security_token": "t"},
        )
        self.assertIn("username", result["connect_args"])
        self.assertNotIn("user", result["connect_args"])

    def test_resolve_oauth_auth(self):
        result = CcpRegistry.resolve(
            "salesforce-crm",
            {
                "consumer_key": "key123",
                "consumer_secret": "secret456",
                "domain": "myorg",
            },
        )
        ca = result["connect_args"]
        self.assertEqual("key123", ca["consumer_key"])
        self.assertEqual("secret456", ca["consumer_secret"])
        self.assertEqual("myorg", ca["domain"])

    def test_domain_suffix_stripped(self):
        result = CcpRegistry.resolve(
            "salesforce-crm",
            {
                "consumer_key": "k",
                "consumer_secret": "s",
                "domain": "myorg.salesforce.com",
            },
        )
        self.assertEqual("myorg", result["connect_args"]["domain"])

    def test_domain_without_suffix_unchanged(self):
        result = CcpRegistry.resolve(
            "salesforce-crm",
            {"consumer_key": "k", "consumer_secret": "s", "domain": "myorg"},
        )
        self.assertEqual("myorg", result["connect_args"]["domain"])

    def test_resolve_legacy_credentials_unchanged(self):
        legacy = {
            "connect_args": {"username": "u", "password": "p", "security_token": "t"}
        }
        self.assertEqual(legacy, CcpRegistry.resolve("salesforce-crm", legacy))
