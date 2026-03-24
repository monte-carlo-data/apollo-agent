# tests/ccp/test_tableau_ccp.py
#
# The proxy client reads credentials flat (not from connect_args) and calls
# generate_jwt internally on each sign-in. Not registered until Phase 2 updates
# TableauProxyClient to read from connect_args.
# Tests use CcpPipeline().execute() directly.
import jwt
from unittest import TestCase

from apollo.integrations.ccp.defaults.tableau import TABLEAU_DEFAULT_CCP
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry

_CREDS = {
    "username": "alice@example.com",
    "client_id": "client-uuid-1234",
    "secret_id": "secret-uuid-5678",
    "secret_value": "supersecret",
    "server_name": "https://tableau.example.com",
}


def _resolve(credentials: dict) -> dict:
    return CcpPipeline().execute(TABLEAU_DEFAULT_CCP, credentials)


class TestTableauCcp(TestCase):
    def test_tableau_not_registered(self):
        self.assertIsNone(CcpRegistry.get("tableau"))

    # ── JWT generation ────────────────────────────────────────────────

    def test_token_is_jwt_string(self):
        args = _resolve(_CREDS)
        self.assertIsInstance(args["token"], str)
        # verify it decodes as a valid HS256 JWT
        payload = jwt.decode(
            args["token"],
            key=_CREDS["secret_value"],
            algorithms=["HS256"],
            audience="tableau",
        )
        self.assertEqual(_CREDS["username"], payload["sub"])
        self.assertEqual(_CREDS["client_id"], payload["iss"])

    def test_token_headers(self):
        args = _resolve(_CREDS)
        header = jwt.get_unverified_header(args["token"])
        self.assertEqual(_CREDS["client_id"], header["iss"])
        self.assertEqual(_CREDS["secret_id"], header["kid"])

    def test_custom_expiration_seconds(self):
        import time

        creds = {**_CREDS, "token_expiration_seconds": 120}
        args = _resolve(creds)
        payload = jwt.decode(
            args["token"],
            key=_CREDS["secret_value"],
            algorithms=["HS256"],
            audience="tableau",
        )
        ttl = payload["exp"] - time.time()
        self.assertLess(ttl, 130)
        self.assertGreater(ttl, 100)

    # ── Server / connection fields ────────────────────────────────────

    def test_server_name_in_output(self):
        args = _resolve(_CREDS)
        self.assertEqual("https://tableau.example.com", args["server_name"])

    def test_site_name_defaults_to_empty_string(self):
        args = _resolve(_CREDS)
        self.assertEqual("", args["site_name"])

    def test_site_name_override(self):
        args = _resolve({**_CREDS, "site_name": "MySite"})
        self.assertEqual("MySite", args["site_name"])

    def test_verify_ssl_defaults_to_true(self):
        args = _resolve(_CREDS)
        self.assertTrue(args["verify_ssl"])

    def test_verify_ssl_override(self):
        args = _resolve({**_CREDS, "verify_ssl": False})
        self.assertFalse(args["verify_ssl"])
