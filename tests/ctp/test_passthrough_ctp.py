# tests/ctp/test_passthrough_ctp.py
#
# PASSTHROUGH_CTP is for customers with simple integrations where credentials
# are passed directly to the proxy client without any transformation.
from unittest import TestCase

from apollo.integrations.ctp.defaults.passthrough import PASSTHROUGH_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(PASSTHROUGH_CTP, credentials)


class TestPassthroughCtp(TestCase):
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
