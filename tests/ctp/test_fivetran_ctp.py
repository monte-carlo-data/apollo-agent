"""
Integration tests for the fivetran connection_type CTP pipeline.

Verifies the self-hosted path (fivetran_api_key + fivetran_api_password → Basic
auth token), the pre-shaped (DC) path, failure handling, and the assertion that
resolved connect_args match what HttpProxyClient consumes.
"""

import base64
from unittest import TestCase

from apollo.integrations.ctp.defaults.fivetran import (
    FIVETRAN_DEFAULT_CTP,
    FivetranClientArgs,
)
from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(FIVETRAN_DEFAULT_CTP, credentials)


_SELF_HOSTED_CREDS = {
    "fivetran_api_key": "actual-key",
    "fivetran_api_password": "actual-password",
}


class TestRegistration(TestCase):
    def test_fivetran_registered(self):
        self.assertIsNotNone(CtpRegistry.get("fivetran"))


class TestSelfHostedPath(TestCase):
    def test_basic_auth_token_encoded(self):
        args = _resolve(_SELF_HOSTED_CREDS)

        expected_token = base64.b64encode(b"actual-key:actual-password").decode()
        self.assertEqual(expected_token, args["token"])

    def test_auth_type_always_basic(self):
        args = _resolve(_SELF_HOSTED_CREDS)

        self.assertEqual("Basic", args["auth_type"])

    def test_special_characters_in_credentials(self):
        creds = {
            "fivetran_api_key": "key:with:colons",
            "fivetran_api_password": "p@ss=word/+",
        }
        args = _resolve(creds)

        expected_token = base64.b64encode(b"key:with:colons:p@ss=word/+").decode()
        self.assertEqual(expected_token, args["token"])
        self.assertEqual("Basic", args["auth_type"])


class TestPreShapedPath(TestCase):
    def test_pre_shaped_token_skips_encoding(self):
        args = _resolve({"token": "pre-computed-b64", "auth_type": "Basic"})

        self.assertEqual("pre-computed-b64", args["token"])
        self.assertEqual("Basic", args["auth_type"])

    def test_pre_shaped_with_extra_credentials_still_skips(self):
        # Even with api_key/password present, an existing token short-circuits
        # the encode_basic_auth transform.
        args = _resolve(
            {
                "token": "pre-computed-b64",
                "fivetran_api_key": "key",
                "fivetran_api_password": "password",
            }
        )

        self.assertEqual("pre-computed-b64", args["token"])


class TestFailurePaths(TestCase):
    def test_missing_api_key_raises(self):
        with self.assertRaises(CtpPipelineError):
            _resolve({"fivetran_api_password": "password"})

    def test_missing_api_password_raises(self):
        with self.assertRaises(CtpPipelineError):
            _resolve({"fivetran_api_key": "key"})

    def test_empty_credentials_raises(self):
        with self.assertRaises(CtpPipelineError):
            _resolve({})


class TestRegistryResolvePath(TestCase):
    """End-to-end through CtpRegistry.resolve — verifies connect_args wrapping."""

    def test_self_hosted_via_registry(self):
        result = CtpRegistry.resolve("fivetran", _SELF_HOSTED_CREDS)

        self.assertIn("connect_args", result)
        args = result["connect_args"]
        expected_token = base64.b64encode(b"actual-key:actual-password").decode()
        self.assertEqual(expected_token, args["token"])
        self.assertEqual("Basic", args["auth_type"])

    def test_pre_shaped_via_registry(self):
        """DC pre-shaped path: credentials arrive wrapped in connect_args."""
        creds = {
            "connect_args": {
                "token": "pre-computed",
                "auth_type": "Basic",
            }
        }
        result = CtpRegistry.resolve("fivetran", creds)

        args = result["connect_args"]
        self.assertEqual("pre-computed", args["token"])
        self.assertEqual("Basic", args["auth_type"])


class TestHttpProxyClientContract(TestCase):
    """Load-bearing bridge between CTP output and HttpProxyClient consumer.
    If FivetranClientArgs drifts from what HttpProxyClient reads, this test
    catches it before the integration fails at runtime.
    """

    def test_connect_args_match_http_proxy_client_contract(self):
        args = _resolve(_SELF_HOSTED_CREDS)

        # HttpProxyClient reads: token, auth_type, auth_header (default), ssl_verify (optional)
        expected_required = {"token", "auth_type"}
        self.assertTrue(
            expected_required.issubset(args.keys()),
            f"Missing required: {expected_required - args.keys()}",
        )
        # Any extra key must be in FivetranClientArgs schema.
        allowed = (
            FivetranClientArgs.__required_keys__ | FivetranClientArgs.__optional_keys__
        )
        unknown = args.keys() - allowed
        self.assertEqual(
            set(),
            unknown,
            f"connect_args has keys outside FivetranClientArgs: {unknown}",
        )
