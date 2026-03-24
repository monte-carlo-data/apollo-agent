# tests/ccp/test_passthrough_ccp.py
#
# PASSTHROUGH_CCP is for customers with simple integrations where credentials
# are passed directly to the proxy client without any transformation.
from unittest import TestCase

from apollo.integrations.ccp.defaults.passthrough import PASSTHROUGH_CCP
from apollo.integrations.ccp.pipeline import CcpPipeline


def _resolve(credentials: dict) -> dict:
    return CcpPipeline().execute(PASSTHROUGH_CCP, credentials)


class TestPassthroughCcp(TestCase):
    def test_all_fields_returned_unchanged(self):
        creds = {
            "host": "db.example.com",
            "port": 5432,
            "user": "alice",
            "password": "secret",
        }
        self.assertEqual(creds, _resolve(creds))

    def test_arbitrary_credential_shape(self):
        creds = {
            "token": "abc123",
            "base_url": "https://api.example.com",
            "verify_ssl": False,
        }
        self.assertEqual(creds, _resolve(creds))

    def test_empty_credentials(self):
        self.assertEqual({}, _resolve({}))
