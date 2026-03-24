# tests/ccp/test_http_ccp.py
#
# HttpProxyClient currently reads credentials flat, so HTTP_DEFAULT_CCP is not
# registered in CcpRegistry._discover(). Tests import the config directly and
# call CcpPipeline().execute() rather than going through CcpRegistry.resolve().
import os
from unittest import TestCase

from apollo.integrations.ccp.defaults.http import HTTP_DEFAULT_CCP
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


def _resolve(credentials: dict) -> dict:
    return CcpPipeline().execute(HTTP_DEFAULT_CCP, credentials)


class TestHttpCcp(TestCase):
    def test_http_not_registered(self):
        # Confirm the config is deliberately absent from the registry.
        self.assertIsNone(CcpRegistry.get("http"))

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
