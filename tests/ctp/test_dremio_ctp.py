# tests/ctp/test_dremio_ctp.py
from unittest import TestCase

from apollo.integrations.ctp.defaults.dremio import DREMIO_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


class TestDremioCtp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CtpRegistry.get("dremio"))

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
        self.assertEqual("mytoken", result["token"])

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

    def test_resolve_pre_shaped_location(self):
        # DC pre-shaped path: connect_args already contains a pre-built location string.
        # CtpRegistry.resolve() unwraps connect_args so raw = {"location": "..."},
        # and the template must pass it through rather than trying to construct from
        # undefined raw.host / raw.port (which would raise UndefinedError).
        result = CtpRegistry.resolve(
            "dremio",
            {
                "connect_args": {"location": "grpc+tls://dremio.example.com:32010"},
                "token": "tok",
            },
        )
        self.assertEqual(
            "grpc+tls://dremio.example.com:32010", result["connect_args"]["location"]
        )
