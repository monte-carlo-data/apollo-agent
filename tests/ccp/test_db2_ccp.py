import os
from unittest import TestCase

from apollo.integrations.ccp.registry import CcpRegistry


def _resolve(credentials: dict) -> dict:
    return CcpRegistry.resolve("db2", credentials)


def _connect_args(credentials: dict) -> dict:
    return _resolve(credentials)["connect_args"]


class TestDb2Ccp(TestCase):
    def test_db2_registered(self):
        config = CcpRegistry.get("db2")
        self.assertIsNotNone(config)
        self.assertEqual("db2-default", config.name)

    def test_resolve_wraps_in_connect_args(self):
        result = _resolve(
            {"host": "db2.example.com", "db_name": "mydb", "user": "u", "password": "p"}
        )
        self.assertIn("connect_args", result)

    # ── Basic connection fields ────────────────────────────────────────

    def test_basic_connection(self):
        args = _connect_args(
            {
                "host": "db2.example.com",
                "db_name": "mydb",
                "user": "alice",
                "password": "secret",
            }
        )
        self.assertEqual("db2.example.com", args["HOSTNAME"])
        self.assertEqual("mydb", args["DATABASE"])
        self.assertEqual("alice", args["UID"])
        self.assertEqual("secret", args["PWD"])
        self.assertEqual("TCPIP", args["PROTOCOL"])

    def test_port_defaults_to_50000(self):
        args = _connect_args(
            {"host": "h", "db_name": "d", "user": "u", "password": "p"}
        )
        self.assertEqual(50000, args["PORT"])

    def test_port_override(self):
        args = _connect_args(
            {"host": "h", "port": 50001, "db_name": "d", "user": "u", "password": "p"}
        )
        self.assertEqual(50001, args["PORT"])

    def test_database_field_alias(self):
        args = _connect_args(
            {"host": "h", "database": "altdb", "user": "u", "password": "p"}
        )
        self.assertEqual("altdb", args["DATABASE"])

    def test_db_name_takes_precedence_over_database(self):
        args = _connect_args(
            {
                "host": "h",
                "db_name": "primary",
                "database": "fallback",
                "user": "u",
                "password": "p",
            }
        )
        self.assertEqual("primary", args["DATABASE"])

    def test_username_field_alias(self):
        args = _connect_args(
            {"host": "h", "db_name": "d", "username": "bob", "password": "p"}
        )
        self.assertEqual("bob", args["UID"])

    # ── Optional timeout fields ────────────────────────────────────────

    def test_query_timeout_maps_to_querytimeout(self):
        args = _connect_args(
            {
                "host": "h",
                "db_name": "d",
                "user": "u",
                "password": "p",
                "query_timeout_in_seconds": 120,
            }
        )
        self.assertEqual(120, args["querytimeout"])

    def test_connect_timeout_maps_to_connecttimeout(self):
        args = _connect_args(
            {
                "host": "h",
                "db_name": "d",
                "user": "u",
                "password": "p",
                "connect_timeout": 30,
            }
        )
        self.assertEqual(30, args["connecttimeout"])

    def test_timeouts_absent_when_not_provided(self):
        args = _connect_args(
            {"host": "h", "db_name": "d", "user": "u", "password": "p"}
        )
        self.assertNotIn("querytimeout", args)
        self.assertNotIn("connecttimeout", args)

    # ── SSL — active ──────────────────────────────────────────────────

    def test_ssl_writes_ca_file_and_sets_security(self):
        pem = b"-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n"
        args = _connect_args(
            {
                "host": "h",
                "db_name": "d",
                "user": "u",
                "password": "p",
                "ssl_options": {"ca_data": pem},
            }
        )
        self.assertEqual("SSL", args["Security"])
        self.assertIn("SSLServerCertificate", args)
        self.assertTrue(os.path.exists(args["SSLServerCertificate"]))
        with open(args["SSLServerCertificate"], "rb") as f:
            self.assertEqual(pem, f.read())
        os.unlink(args["SSLServerCertificate"])

    def test_ssl_no_ca_data_skips_ssl_step(self):
        args = _connect_args(
            {
                "host": "h",
                "db_name": "d",
                "user": "u",
                "password": "p",
                "ssl_options": {},
            }
        )
        self.assertNotIn("Security", args)
        self.assertNotIn("SSLServerCertificate", args)

    def test_ssl_disabled_flag_skips_ssl_step(self):
        pem = b"FAKE_CERT"
        args = _connect_args(
            {
                "host": "h",
                "db_name": "d",
                "user": "u",
                "password": "p",
                "ssl_options": {"ca_data": pem, "disabled": True},
            }
        )
        self.assertNotIn("Security", args)
        self.assertNotIn("SSLServerCertificate", args)

    # ── Legacy passthrough ────────────────────────────────────────────

    def test_legacy_connect_args_passthrough(self):
        legacy = {
            "connect_args": {
                "DATABASE": "testdb",
                "HOSTNAME": "localhost",
                "PORT": "50000",
                "PROTOCOL": "TCPIP",
                "UID": "testuser",
                "PWD": "testpass",
            }
        }
        self.assertEqual(legacy, _resolve(legacy))
