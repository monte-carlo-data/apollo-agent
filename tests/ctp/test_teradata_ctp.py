import os
from unittest import TestCase

from apollo.integrations.ctp.defaults.teradata import TERADATA_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(TERADATA_DEFAULT_CTP, credentials)


class TestTeradataCtp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CtpRegistry.get("teradata"))

    # ── Basic connection fields ────────────────────────────────────────

    def test_basic_connection(self):
        args = _resolve(
            {"host": "td.example.com", "user": "alice", "password": "secret"}
        )
        self.assertEqual("td.example.com", args["host"])
        self.assertEqual("alice", args["user"])
        self.assertEqual("secret", args["password"])

    def test_plain_port_maps_to_dbs_port(self):
        args = _resolve(
            {"host": "td.example.com", "user": "u", "password": "p", "port": 1025}
        )
        self.assertEqual(1025, args["dbs_port"])
        self.assertNotIn("https_port", args)

    def test_no_port_omits_dbs_port(self):
        args = _resolve({"host": "td.example.com", "user": "u", "password": "p"})
        self.assertNotIn("dbs_port", args)
        self.assertNotIn("https_port", args)

    def test_dc_shaped_dbs_port_maps_to_dbs_port(self):
        # The data-collector sends the port pre-shaped as `dbs_port` inside
        # connect_args; registry.py unwraps connect_args as `raw`, so `raw`
        # carries `dbs_port` (not `port`). The template must honor that key.
        args = _resolve(
            {"host": "td.example.com", "user": "u", "password": "p", "dbs_port": 1025}
        )
        self.assertEqual(1025, args["dbs_port"])
        self.assertNotIn("https_port", args)

    # ── Protocol option defaults ───────────────────────────────────────

    def test_tmode_defaults_to_TERA(self):
        args = _resolve({"host": "h", "user": "u", "password": "p"})
        self.assertEqual("TERA", args["tmode"])

    def test_sslmode_defaults_to_PREFER(self):
        args = _resolve({"host": "h", "user": "u", "password": "p"})
        self.assertEqual("PREFER", args["sslmode"])

    def test_logmech_defaults_to_TD2(self):
        args = _resolve({"host": "h", "user": "u", "password": "p"})
        self.assertEqual("TD2", args["logmech"])

    def test_tmode_override(self):
        args = _resolve({"host": "h", "user": "u", "password": "p", "tmode": "ANSI"})
        self.assertEqual("ANSI", args["tmode"])

    def test_sslmode_override(self):
        args = _resolve(
            {"host": "h", "user": "u", "password": "p", "sslmode": "REQUIRE"}
        )
        self.assertEqual("REQUIRE", args["sslmode"])

    def test_logmech_override(self):
        args = _resolve({"host": "h", "user": "u", "password": "p", "logmech": "LDAP"})
        self.assertEqual("LDAP", args["logmech"])

    # ── Timeout field renames ─────────────────────────────────────────

    def test_query_timeout_maps_to_request_timeout(self):
        args = _resolve(
            {"host": "h", "user": "u", "password": "p", "query_timeout_in_seconds": 60}
        )
        self.assertEqual(60, args["request_timeout"])

    def test_login_timeout_maps_to_logon_timeout(self):
        args = _resolve(
            {"host": "h", "user": "u", "password": "p", "login_timeout_in_seconds": 30}
        )
        self.assertEqual(30, args["logon_timeout"])

    def test_timeouts_absent_when_not_provided(self):
        args = _resolve({"host": "h", "user": "u", "password": "p"})
        self.assertNotIn("request_timeout", args)
        self.assertNotIn("logon_timeout", args)

    # ── SSL — active (ca_data present, not disabled) ──────────────────

    def test_ssl_writes_ca_file_and_sets_sslca(self):
        pem = b"-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n"
        args = _resolve(
            {
                "host": "h",
                "user": "u",
                "password": "p",
                "port": 443,
                "ssl_options": {"ca_data": pem},
            }
        )
        self.assertIn("sslca", args)
        self.assertTrue(os.path.exists(args["sslca"]))
        with open(args["sslca"], "rb") as f:
            self.assertEqual(pem, f.read())
        os.unlink(args["sslca"])

    def test_ssl_sets_encryptdata_string_true(self):
        pem = b"FAKE_CERT"
        args = _resolve(
            {
                "host": "h",
                "user": "u",
                "password": "p",
                "port": 443,
                "ssl_options": {"ca_data": pem},
            }
        )
        self.assertEqual("true", args["encryptdata"])
        self.assertIsInstance(args["encryptdata"], str)
        if os.path.exists(args.get("sslca", "")):
            os.unlink(args["sslca"])

    def test_ssl_uses_https_port_not_dbs_port(self):
        pem = b"FAKE_CERT"
        args = _resolve(
            {
                "host": "h",
                "user": "u",
                "password": "p",
                "port": 443,
                "ssl_options": {"ca_data": pem},
            }
        )
        self.assertEqual(443, args["https_port"])
        self.assertNotIn("dbs_port", args)
        if os.path.exists(args.get("sslca", "")):
            os.unlink(args["sslca"])

    def test_ssl_no_port_omits_https_port(self):
        # SSL active (ca_data present, not disabled) but no port supplied.
        # The SSL step must guard https_port the same way the mapper guards
        # dbs_port, otherwise the StrictUndefined render raises
        # UndefinedError: 'port' is undefined and validate_connection crashes.
        pem = b"FAKE_CERT"
        args = _resolve(
            {
                "host": "h",
                "user": "u",
                "password": "p",
                "ssl_options": {"ca_data": pem},
            }
        )
        sslca_path = args.get("sslca")
        if sslca_path:
            self.addCleanup(lambda p=sslca_path: os.path.exists(p) and os.unlink(p))
        # SSL is still active: CA file written and encryptdata set.
        self.assertIn("sslca", args)
        self.assertEqual("true", args["encryptdata"])
        # No port supplied -> neither port key is emitted, so teradatasql
        # falls back to its own SSL default instead of the agent crashing.
        self.assertNotIn("https_port", args)
        self.assertNotIn("dbs_port", args)

    def test_ssl_dc_shaped_dbs_port_used_as_https_port(self):
        # Customer-reported case: data-collector supplies the port as
        # `dbs_port` (not `port`) AND SSL is active. The SSL step must pick
        # up dbs_port as the https_port instead of silently dropping it.
        pem = b"FAKE_CERT"
        args = _resolve(
            {
                "host": "h",
                "user": "u",
                "password": "p",
                "dbs_port": 1025,
                "ssl_options": {"ca_data": pem},
            }
        )
        sslca_path = args.get("sslca")
        if sslca_path:
            self.addCleanup(lambda p=sslca_path: os.path.exists(p) and os.unlink(p))
        self.assertEqual(1025, args["https_port"])
        self.assertNotIn("dbs_port", args)
        self.assertIn("sslca", args)
        self.assertEqual("true", args["encryptdata"])

    # ── SSL — disabled flag ───────────────────────────────────────────

    def test_ssl_disabled_flag_skips_ssl_step(self):
        pem = b"FAKE_CERT"
        args = _resolve(
            {
                "host": "h",
                "user": "u",
                "password": "p",
                "port": 1025,
                "ssl_options": {"ca_data": pem, "disabled": True},
            }
        )
        self.assertNotIn("sslca", args)
        self.assertNotIn("encryptdata", args)
        self.assertNotIn("https_port", args)
        # Plain port still emitted
        self.assertEqual(1025, args["dbs_port"])

    def test_ssl_no_ca_data_skips_ssl_step(self):
        args = _resolve(
            {
                "host": "h",
                "user": "u",
                "password": "p",
                "port": 1025,
                "ssl_options": {},
            }
        )
        self.assertNotIn("sslca", args)
        self.assertNotIn("encryptdata", args)
        self.assertNotIn("https_port", args)
        self.assertEqual(1025, args["dbs_port"])
