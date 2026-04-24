from unittest import TestCase

from apollo.integrations.ctp.defaults.bigquery import BIGQUERY_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry

_SA_JSON = {
    "type": "service_account",
    "project_id": "my-project",
    "private_key_id": "key123",
    "private_key": "-----BEGIN RSA PRIVATE KEY-----\nFAKE\n-----END RSA PRIVATE KEY-----\n",
    "client_email": "sa@my-project.iam.gserviceaccount.com",
    "client_id": "123456789",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/sa",
}


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(BIGQUERY_DEFAULT_CTP, credentials)


class TestBigqueryCtp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CtpRegistry.get("bigquery"))

    # ── Service account JSON passthrough ──────────────────────────────

    def test_service_account_fields_passed_through(self):
        args = _resolve(_SA_JSON)
        self.assertEqual("service_account", args["type"])
        self.assertEqual("my-project", args["project_id"])
        self.assertEqual("key123", args["private_key_id"])
        self.assertEqual("sa@my-project.iam.gserviceaccount.com", args["client_email"])
        self.assertEqual("123456789", args["client_id"])
        self.assertEqual("https://accounts.google.com/o/oauth2/auth", args["auth_uri"])
        self.assertEqual("https://oauth2.googleapis.com/token", args["token_uri"])

    def test_absent_fields_omitted(self):
        # Partial credentials — only project_id and type
        args = _resolve({"type": "service_account", "project_id": "proj"})
        self.assertEqual("service_account", args["type"])
        self.assertEqual("proj", args["project_id"])
        for field in ("private_key_id", "private_key", "client_email", "client_id"):
            self.assertNotIn(field, args, f"expected {field!r} to be absent")

    # ── ADC fallback (no credentials) ────────────────────────────────

    def test_empty_credentials_produce_empty_connect_args(self):
        # When no SA JSON is provided the proxy client uses ADC.
        # CTP produces an empty connect_args dict; proxy client skips
        # Credentials.from_service_account_info() because the dict is falsy.
        args = _resolve({})
        self.assertEqual({}, args)

    # ── socket_timeout_in_seconds ─────────────────────────────────────

    def test_socket_timeout_included(self):
        args = _resolve({**_SA_JSON, "socket_timeout_in_seconds": 30})
        self.assertEqual(30, args["socket_timeout_in_seconds"])

    def test_socket_timeout_absent_when_not_provided(self):
        args = _resolve(_SA_JSON)
        self.assertNotIn("socket_timeout_in_seconds", args)
