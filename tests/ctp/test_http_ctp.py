# tests/ctp/test_http_ctp.py
#
# HttpProxyClient currently reads credentials flat, so HTTP_DEFAULT_CTP is not
# registered in CtpRegistry._discover(). Tests import the config directly and
# call CtpPipeline().execute() rather than going through CtpRegistry.resolve().
import os
from unittest import TestCase

from apollo.integrations.ctp.defaults.http import HTTP_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(HTTP_DEFAULT_CTP, credentials)


class TestHttpCtp(TestCase):
    def test_http_not_registered(self):
        # Confirm the config is deliberately absent from the registry.
        self.assertIsNone(CtpRegistry.get("http"))

    def test_resolve_token_auth(self):
        result = _resolve({"token": "mytoken"})
        self.assertEqual("mytoken", result["token"])

    def test_resolve_custom_auth_header_and_type(self):
        result = _resolve(
            {
                "token": "mytoken",
                "auth_header": "X-Api-Key",
                "auth_type": "Token",
            }
        )
        self.assertEqual("X-Api-Key", result["auth_header"])
        self.assertEqual("Token", result["auth_type"])

    def test_omits_absent_optional_fields(self):
        result = _resolve({"token": "t"})
        self.assertNotIn("auth_header", result)
        self.assertNotIn("auth_type", result)
        self.assertNotIn("ssl_verify", result)

    def test_ssl_disabled(self):
        result = _resolve({"token": "t", "ssl_options": {"disabled": True}})
        self.assertIs(False, result["ssl_verify"])

    def test_ssl_ca_data_writes_temp_file(self):
        result = _resolve(
            {
                "token": "t",
                "ssl_options": {
                    "ca_data": "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n"
                },
            }
        )
        self.assertIn("ssl_verify", result)
        self.assertIsInstance(result["ssl_verify"], str)
        self.assertTrue(result["ssl_verify"].endswith(".pem"))
        if os.path.exists(result["ssl_verify"]):
            os.unlink(result["ssl_verify"])

    def test_no_ssl_options_no_ssl_verify(self):
        result = _resolve({"token": "t"})
        self.assertNotIn("ssl_verify", result)
