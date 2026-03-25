from unittest import TestCase

from apollo.credentials.base import BaseCredentialsService
from apollo.integrations.ctp.defaults.passthrough import PASSTHROUGH_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


class TestBaseCredentialsService(TestCase):
    def test_plain_credentials_returned_unchanged(self):
        svc = BaseCredentialsService()
        creds = {"connect_args": {"host": "h", "port": 5432}}
        result = svc.get_credentials(creds)
        self.assertEqual({"connect_args": {"host": "h", "port": 5432}}, result)


class TestPassthroughCtp(TestCase):
    def test_passthrough_pipeline_returns_raw(self):
        raw = {"host": "h", "port": 5432, "user": "u", "password": "p"}
        result = CtpPipeline().execute(PASSTHROUGH_CTP, raw)
        self.assertEqual(raw, result)

    def test_passthrough_registered_connector_wraps_in_connect_args(self):
        CtpRegistry.register("_test_passthrough", PASSTHROUGH_CTP)
        result = CtpRegistry.resolve("_test_passthrough", {"host": "h", "port": 5432})
        self.assertIn("connect_args", result)
        self.assertEqual("h", result["connect_args"]["host"])
