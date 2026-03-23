# tests/ccp/test_dremio_ccp.py
from unittest import TestCase

from apollo.integrations.ccp.defaults.dremio import DREMIO_DEFAULT_CCP
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


class TestDremioCcp(TestCase):
    def test_not_registered(self):
        self.assertIsNone(CcpRegistry.get("dremio"))

    def test_resolve_plain_grpc(self):
        result = CcpPipeline().execute(
            DREMIO_DEFAULT_CCP,
            {
                "host": "dremio.example.com",
                "port": 32010,
                "token": "mytoken",
            },
        )
        self.assertEqual("grpc://dremio.example.com:32010", result["location"])
        # token is not in connect_args — proxy client reads it from top-level until Phase 2
        self.assertNotIn("token", result)

    def test_resolve_grpc_tls(self):
        result = CcpPipeline().execute(
            DREMIO_DEFAULT_CCP,
            {
                "host": "dremio.example.com",
                "port": 32010,
                "token": "mytoken",
                "use_tls": True,
            },
        )
        self.assertEqual("grpc+tls://dremio.example.com:32010", result["location"])

    def test_no_tls_flag_uses_plain_grpc(self):
        result = CcpPipeline().execute(
            DREMIO_DEFAULT_CCP,
            {"host": "h", "port": 32010, "token": "t"},
        )
        self.assertTrue(result["location"].startswith("grpc://"))
        self.assertFalse(result["location"].startswith("grpc+tls://"))
