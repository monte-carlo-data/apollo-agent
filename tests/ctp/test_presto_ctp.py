# tests/ctp/test_presto_ctp.py
import prestodb
from unittest import TestCase

from apollo.integrations.ctp.defaults.presto import PRESTO_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(PRESTO_DEFAULT_CTP, credentials)


class TestPrestoCtp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CtpRegistry.get("presto"))

    # ── Basic connection fields ────────────────────────────────────────

    def test_basic_connection(self):
        args = _resolve({"host": "presto.example.com", "port": 8889, "user": "alice"})
        self.assertEqual("presto.example.com", args["host"])
        self.assertEqual(8889, args["port"])
        self.assertEqual("alice", args["user"])

    def test_port_defaults_to_8889(self):
        args = _resolve({"host": "h", "user": "u"})
        self.assertEqual(8889, args["port"])

    def test_http_scheme_defaults_to_http(self):
        args = _resolve({"host": "h", "user": "u"})
        self.assertEqual("http", args["http_scheme"])

    def test_http_scheme_override(self):
        args = _resolve({"host": "h", "user": "u", "http_scheme": "https"})
        self.assertEqual("https", args["http_scheme"])

    def test_max_attempts_always_3(self):
        args = _resolve({"host": "h", "user": "u"})
        self.assertEqual(3, args["max_attempts"])

    def test_username_field_alias(self):
        args = _resolve({"host": "h", "username": "bob"})
        self.assertEqual("bob", args["user"])

    # ── Optional fields ───────────────────────────────────────────────

    def test_catalog_and_schema(self):
        args = _resolve(
            {"host": "h", "user": "u", "catalog": "hive", "schema": "default"}
        )
        self.assertEqual("hive", args["catalog"])
        self.assertEqual("default", args["schema"])

    def test_request_timeout(self):
        args = _resolve({"host": "h", "user": "u", "request_timeout": 120})
        self.assertEqual(120, args["request_timeout"])

    def test_omits_absent_optional_fields(self):
        args = _resolve({"host": "h", "user": "u"})
        for field in ("catalog", "schema", "request_timeout"):
            self.assertNotIn(field, args, f"expected {field!r} to be absent")

    # ── Auth passthrough ──────────────────────────────────────────────

    def test_auth_produces_basic_authentication_object(self):
        auth = {"username": "alice", "password": "secret"}
        args = _resolve({"host": "h", "user": "u", "auth": auth})
        self.assertIsInstance(args["auth"], prestodb.auth.BasicAuthentication)

    def test_auth_absent_when_not_provided(self):
        # When auth is not in raw credentials, the step is skipped (when guard).
        args = _resolve({"host": "h", "user": "u"})
        self.assertNotIn("auth", args)
