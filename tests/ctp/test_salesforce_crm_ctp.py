# tests/ctp/test_salesforce_crm_ctp.py
from unittest import TestCase

from apollo.integrations.ctp.defaults.salesforce_crm import SALESFORCE_CRM_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


class TestSalesforceCrmCtp(TestCase):
    def test_not_registered(self):
        self.assertIsNone(CtpRegistry.get("salesforce-crm"))

    def test_resolve_token_auth(self):
        result = CtpPipeline().execute(
            SALESFORCE_CRM_DEFAULT_CTP,
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
        result = CtpPipeline().execute(
            SALESFORCE_CRM_DEFAULT_CTP,
            {"user": "admin@example.com", "password": "p", "security_token": "t"},
        )
        self.assertIn("username", result)
        self.assertNotIn("user", result)

    def test_resolve_oauth_auth(self):
        result = CtpPipeline().execute(
            SALESFORCE_CRM_DEFAULT_CTP,
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
        result = CtpPipeline().execute(
            SALESFORCE_CRM_DEFAULT_CTP,
            {
                "consumer_key": "k",
                "consumer_secret": "s",
                "domain": "myorg.salesforce.com",
            },
        )
        self.assertEqual("myorg", result["domain"])

    def test_domain_without_suffix_unchanged(self):
        result = CtpPipeline().execute(
            SALESFORCE_CRM_DEFAULT_CTP,
            {"consumer_key": "k", "consumer_secret": "s", "domain": "myorg"},
        )
        self.assertEqual("myorg", result["domain"])
