# tests/ctp/test_looker_ctp.py
#
# Not registered: proxy client reads flat credentials and writes the looker-sdk INI
# file itself; CTP output would break that flow. Phase 2 adds a write_ini_file
# transform and updates the proxy client. Tests use CtpPipeline().execute() directly.
from unittest import TestCase

from apollo.integrations.ctp.defaults.looker import LOOKER_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(LOOKER_DEFAULT_CTP, credentials)


class TestLookerCtp(TestCase):
    def test_looker_not_registered(self):
        self.assertIsNone(CtpRegistry.get("looker"))

    def test_basic_connection(self):
        args = _resolve(
            {
                "base_url": "https://mycompany.looker.com",
                "client_id": "abc123",
                "client_secret": "supersecret",
            }
        )
        self.assertEqual("https://mycompany.looker.com", args["base_url"])
        self.assertEqual("abc123", args["client_id"])
        self.assertEqual("supersecret", args["client_secret"])

    def test_verify_ssl_defaults_to_true(self):
        args = _resolve(
            {
                "base_url": "https://mycompany.looker.com",
                "client_id": "id",
                "client_secret": "secret",
            }
        )
        self.assertTrue(args["verify_ssl"])

    def test_verify_ssl_override(self):
        args = _resolve(
            {
                "base_url": "https://mycompany.looker.com",
                "client_id": "id",
                "client_secret": "secret",
                "verify_ssl": False,
            }
        )
        self.assertFalse(args["verify_ssl"])
