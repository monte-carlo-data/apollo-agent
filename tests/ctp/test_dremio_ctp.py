# tests/ctp/test_dremio_ctp.py
from unittest import TestCase

from apollo.integrations.ctp.defaults.dremio import DREMIO_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


class TestDremioCtp(TestCase):
    def test_not_registered(self):
        self.assertIsNone(CtpRegistry.get("dremio"))

    def test_resolve_plain_grpc(self):
        result = CtpPipeline().execute(
            DREMIO_DEFAULT_CTP,
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
        result = CtpPipeline().execute(
            DREMIO_DEFAULT_CTP,
            {
                "host": "dremio.example.com",
                "port": 32010,
                "token": "mytoken",
                "use_tls": True,
            },
        )
        self.assertEqual("grpc+tls://dremio.example.com:32010", result["location"])

    def test_no_tls_flag_uses_plain_grpc(self):
        result = CtpPipeline().execute(
            DREMIO_DEFAULT_CTP,
            {"host": "h", "port": 32010, "token": "t"},
        )
        self.assertTrue(result["location"].startswith("grpc://"))
        self.assertFalse(result["location"].startswith("grpc+tls://"))
