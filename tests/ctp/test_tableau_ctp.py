# tests/ctp/test_tableau_ctp.py
from unittest import TestCase

from apollo.integrations.ctp.defaults.tableau import TABLEAU_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry

_CONNECTED_APP_CREDS = {
    "username": "alice@example.com",
    "client_id": "client-uuid-1234",
    "secret_id": "secret-uuid-5678",
    "secret_value": "supersecret",
    "server_name": "https://tableau.example.com",
}


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(TABLEAU_DEFAULT_CTP, credentials)


class TestTableauCtp(TestCase):
    def test_tableau_registered(self):
        self.assertIsNotNone(CtpRegistry.get("tableau"))

    # ── Flat credentials (Connected App) path ────────────────────────────

    def test_connected_app_fields_passed_through(self):
        # JWT is generated per sign-in by the proxy client; CTP passes raw fields through.
        args = _resolve(_CONNECTED_APP_CREDS)
        self.assertEqual("client-uuid-1234", args["client_id"])
        self.assertEqual("secret-uuid-5678", args["secret_id"])
        self.assertEqual("supersecret", args["secret_value"])
        self.assertEqual("alice@example.com", args["username"])
        self.assertIsNone(args.get("token"))

    def test_custom_expiration_seconds_passed_through(self):
        args = _resolve({**_CONNECTED_APP_CREDS, "token_expiration_seconds": 120})
        self.assertEqual(120, args["token_expiration_seconds"])

    def test_expiration_seconds_defaults_to_none(self):
        args = _resolve(_CONNECTED_APP_CREDS)
        self.assertIsNone(args.get("token_expiration_seconds"))

    # ── DC pre-shaped path ───────────────────────────────────────────────

    def test_pre_shaped_token_passed_through(self):
        args = _resolve(
            {"server_name": "https://tableau.example.com", "token": "pre.built.jwt"}
        )
        self.assertEqual("pre.built.jwt", args["token"])
        self.assertIsNone(args.get("client_id"))

    # ── Server / connection fields ────────────────────────────────────────

    def test_server_name_in_output(self):
        args = _resolve(_CONNECTED_APP_CREDS)
        self.assertEqual("https://tableau.example.com", args["server_name"])

    def test_site_name_defaults_to_empty_string(self):
        args = _resolve(_CONNECTED_APP_CREDS)
        self.assertEqual("", args["site_name"])

    def test_site_name_override(self):
        args = _resolve({**_CONNECTED_APP_CREDS, "site_name": "MySite"})
        self.assertEqual("MySite", args["site_name"])

    def test_verify_ssl_defaults_to_true(self):
        args = _resolve(_CONNECTED_APP_CREDS)
        self.assertTrue(args["verify_ssl"])

    def test_verify_ssl_override(self):
        args = _resolve({**_CONNECTED_APP_CREDS, "verify_ssl": False})
        self.assertFalse(args["verify_ssl"])
