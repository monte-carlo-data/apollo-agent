# tests/ccp/test_dremio_ccp.py
from unittest import TestCase

from apollo.integrations.ccp.registry import CcpRegistry


class TestDremioCcp(TestCase):
    def test_dremio_registered(self):
        config = CcpRegistry.get("dremio")
        self.assertIsNotNone(config)
        self.assertEqual("dremio-default", config.name)

    def test_resolve_plain_grpc(self):
        result = CcpRegistry.resolve(
            "dremio",
            {
                "host": "dremio.example.com",
                "port": 32010,
                "token": "mytoken",
            },
        )
        self.assertIn("connect_args", result)
        ca = result["connect_args"]
        self.assertEqual("grpc://dremio.example.com:32010", ca["location"])
        # token is not in connect_args — proxy client reads it from top-level until Phase 2
        self.assertNotIn("token", ca)

    def test_resolve_grpc_tls(self):
        result = CcpRegistry.resolve(
            "dremio",
            {
                "host": "dremio.example.com",
                "port": 32010,
                "token": "mytoken",
                "use_tls": True,
            },
        )
        self.assertEqual(
            "grpc+tls://dremio.example.com:32010", result["connect_args"]["location"]
        )

    def test_no_tls_flag_uses_plain_grpc(self):
        result = CcpRegistry.resolve(
            "dremio",
            {"host": "h", "port": 32010, "token": "t"},
        )
        self.assertTrue(result["connect_args"]["location"].startswith("grpc://"))
        self.assertFalse(result["connect_args"]["location"].startswith("grpc+tls://"))

    def test_resolve_legacy_credentials_unchanged(self):
        legacy = {"connect_args": {"location": "grpc://h:32010", "token": "t"}}
        self.assertEqual(legacy, CcpRegistry.resolve("dremio", legacy))
