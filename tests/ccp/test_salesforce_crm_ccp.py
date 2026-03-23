# tests/ccp/test_salesforce_crm_ccp.py
from unittest import TestCase

from apollo.integrations.ccp.defaults.salesforce_crm import SALESFORCE_CRM_DEFAULT_CCP
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


class TestSalesforceCrmCcp(TestCase):
    def test_not_registered(self):
        self.assertIsNone(CcpRegistry.get("salesforce-crm"))

    def test_resolve_token_auth(self):
        result = CcpPipeline().execute(
            SALESFORCE_CRM_DEFAULT_CCP,
            {
                "user": "admin@example.com",
                "password": "secret",
                "security_token": "ABC123",
            },
        )
        self.assertEqual("admin@example.com", result["username"])
        self.assertEqual("secret", result["password"])
        self.assertEqual("ABC123", result["security_token"])
        self.assertNotIn("consumer_key", result)
        self.assertNotIn("domain", result)

    def test_user_mapped_to_username(self):
        result = CcpPipeline().execute(
            SALESFORCE_CRM_DEFAULT_CCP,
            {"user": "admin@example.com", "password": "p", "security_token": "t"},
        )
        self.assertIn("username", result)
        self.assertNotIn("user", result)

    def test_resolve_oauth_auth(self):
        result = CcpPipeline().execute(
            SALESFORCE_CRM_DEFAULT_CCP,
            {
                "consumer_key": "key123",
                "consumer_secret": "secret456",
                "domain": "myorg",
            },
        )
        self.assertEqual("key123", result["consumer_key"])
        self.assertEqual("secret456", result["consumer_secret"])
        self.assertEqual("myorg", result["domain"])

    def test_domain_suffix_stripped(self):
        result = CcpPipeline().execute(
            SALESFORCE_CRM_DEFAULT_CCP,
            {
                "consumer_key": "k",
                "consumer_secret": "s",
                "domain": "myorg.salesforce.com",
            },
        )
        self.assertEqual("myorg", result["domain"])

    def test_domain_without_suffix_unchanged(self):
        result = CcpPipeline().execute(
            SALESFORCE_CRM_DEFAULT_CCP,
            {"consumer_key": "k", "consumer_secret": "s", "domain": "myorg"},
        )
        self.assertEqual("myorg", result["domain"])
