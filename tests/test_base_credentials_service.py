import base64
from unittest import TestCase

from apollo.credentials.base import BaseCredentialsService
from apollo.integrations.ccp.defaults.passthrough import PASSTHROUGH_CCP
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


class TestBaseCredentialsServiceDecode(TestCase):
    """Verify decode_dictionary runs after _merge_connect_args."""

    def test_plain_credentials_returned_unchanged(self):
        svc = BaseCredentialsService()
        creds = {"connect_args": {"host": "h", "port": 5432}}
        result = svc.get_credentials(creds)
        self.assertEqual({"connect_args": {"host": "h", "port": 5432}}, result)

    def test_binary_value_decoded(self):
        encoded = {
            "__type__": "bytes",
            "__data__": base64.b64encode(b"raw-cert").decode(),
        }
        svc = BaseCredentialsService()
        result = svc.get_credentials({"connect_args": {"cert": encoded}})
        self.assertEqual(b"raw-cert", result["connect_args"]["cert"])


class TestPassthroughCcp(TestCase):
    def test_passthrough_pipeline_returns_raw(self):
        raw = {"host": "h", "port": 5432, "user": "u", "password": "p"}
        result = CcpPipeline().execute(PASSTHROUGH_CCP, raw)
        self.assertEqual(raw, result)

    def test_passthrough_registered_connector_wraps_in_connect_args(self):
        CcpRegistry.register("_test_passthrough", PASSTHROUGH_CCP)
        result = CcpRegistry.resolve("_test_passthrough", {"host": "h", "port": 5432})
        self.assertIn("connect_args", result)
        self.assertEqual("h", result["connect_args"]["host"])
