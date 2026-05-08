"""
Integration tests for the mulesoft connection_type CTP pipeline.

Verifies the two-step pipeline (resolve_mulesoft_endpoints → oauth) end-to-end,
the pre-shaped (DC) path, failure handling, and the load-bearing assertion that
the resolved connect_args match what HttpProxyClient consumes.
"""

from unittest import TestCase
from unittest.mock import MagicMock, patch

import requests

from apollo.integrations.ctp.defaults.mulesoft import (
    MULESOFT_DEFAULT_CTP,
    MulesoftClientArgs,
)
from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(MULESOFT_DEFAULT_CTP, credentials)


def _mock_token_response(access_token: str = "tok") -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {"access_token": access_token}
    resp.raise_for_status.return_value = None
    return resp


_FLAT_CREDS = {"client_id": "cid", "client_secret": "csec"}


class TestRegistration(TestCase):
    def test_mulesoft_registered(self):
        self.assertIsNotNone(CtpRegistry.get("mulesoft"))


class TestRegionFullPipeline(TestCase):
    @patch("apollo.integrations.ctp.transforms.oauth.requests")
    def test_us_default_region_full_pipeline(self, mock_requests):
        mock_requests.post.return_value = _mock_token_response("us-tok")

        args = _resolve(_FLAT_CREDS)

        self.assertEqual("us-tok", args["token"])
        self.assertEqual("Bearer", args["auth_type"])
        self.assertNotIn("ssl_verify", args)
        # Verify OAuth POSTed to the US region endpoint.
        post_args = mock_requests.post.call_args
        self.assertEqual(
            "https://anypoint.mulesoft.com/accounts/api/v2/oauth2/token",
            post_args.args[0],
        )

    @patch("apollo.integrations.ctp.transforms.oauth.requests")
    def test_eu_region_pipeline(self, mock_requests):
        mock_requests.post.return_value = _mock_token_response("eu-tok")

        args = _resolve({**_FLAT_CREDS, "region": "EU"})

        self.assertEqual("eu-tok", args["token"])
        self.assertEqual(
            "https://eu1.anypoint.mulesoft.com/accounts/api/v2/oauth2/token",
            mock_requests.post.call_args.args[0],
        )

    @patch("apollo.integrations.ctp.transforms.oauth.requests")
    def test_gov_region_pipeline(self, mock_requests):
        mock_requests.post.return_value = _mock_token_response("gov-tok")

        args = _resolve({**_FLAT_CREDS, "region": "Gov"})

        self.assertEqual("gov-tok", args["token"])
        self.assertEqual(
            "https://mpt.mulesoft.com/accounts/api/v2/oauth2/token",
            mock_requests.post.call_args.args[0],
        )

    @patch("apollo.integrations.ctp.transforms.oauth.requests")
    def test_auth_type_always_bearer(self, mock_requests):
        mock_requests.post.return_value = _mock_token_response()

        args = _resolve(_FLAT_CREDS)

        self.assertEqual("Bearer", args["auth_type"])


class TestPreShapedPath(TestCase):
    @patch("apollo.integrations.ctp.transforms.oauth.requests")
    def test_pre_shaped_token_skips_both_steps(self, mock_requests):
        args = _resolve({"token": "pre-shaped"})

        self.assertEqual("pre-shaped", args["token"])
        self.assertEqual("Bearer", args["auth_type"])
        mock_requests.post.assert_not_called()

    @patch("apollo.integrations.ctp.transforms.oauth.requests")
    def test_pre_shaped_with_extra_credentials_still_skips(self, mock_requests):
        # Even with full client_id/secret present, an existing token short-circuits
        # both the endpoints transform and the oauth transform.
        args = _resolve(
            {
                "token": "pre-shaped",
                "client_id": "cid",
                "client_secret": "csec",
            }
        )

        self.assertEqual("pre-shaped", args["token"])
        mock_requests.post.assert_not_called()


class TestFailurePaths(TestCase):
    @patch("apollo.integrations.ctp.transforms.oauth.requests")
    def test_oauth_failure_raises(self, mock_requests):
        # Simulate a 401 from the token endpoint.
        fail_resp = MagicMock()
        fail_resp.status_code = 401
        http_error = requests.HTTPError(response=fail_resp)
        fail_resp.raise_for_status.side_effect = http_error
        mock_requests.HTTPError = requests.HTTPError
        mock_requests.RequestException = requests.RequestException
        mock_requests.post.return_value = fail_resp

        with self.assertRaises(CtpPipelineError):
            _resolve(_FLAT_CREDS)

    @patch("apollo.integrations.ctp.transforms.oauth.requests")
    def test_invalid_region_raises_before_oauth(self, mock_requests):
        with self.assertRaises(CtpPipelineError) as ctx:
            _resolve({**_FLAT_CREDS, "region": "APAC"})

        self.assertIn("APAC", str(ctx.exception))
        # Pipeline must halt at the endpoints transform — OAuth must not be
        # attempted with the wrong (or undetermined) token endpoint.
        mock_requests.post.assert_not_called()


class TestSslPassthrough(TestCase):
    @patch("apollo.integrations.ctp.transforms.oauth.requests")
    def test_ssl_verify_passed_through(self, mock_requests):
        mock_requests.post.return_value = _mock_token_response()

        args = _resolve({**_FLAT_CREDS, "ssl_verify": "/path/to/ca-bundle.crt"})

        self.assertEqual("/path/to/ca-bundle.crt", args["ssl_verify"])


class TestHttpProxyClientContract(TestCase):
    """Load-bearing bridge between Phase 2 (CTP output) and Phase 3 (HttpProxyClient
    consumer). If MulesoftClientArgs drifts from what HttpProxyClient reads, this
    test catches it before the integration fails at runtime.
    """

    @patch("apollo.integrations.ctp.transforms.oauth.requests")
    def test_connect_args_match_http_proxy_client_contract(self, mock_requests):
        mock_requests.post.return_value = _mock_token_response()

        args = _resolve(_FLAT_CREDS)

        # HttpProxyClient reads from connect_args: token, auth_type, auth_header
        # (defaulted to "Authorization"), ssl_verify (optional). The mapper must
        # produce exactly the keys our contract calls for — no more (silent typo
        # masking), no less (missing-field mapper failure).
        expected_required = {"token", "auth_type"}
        self.assertTrue(
            expected_required.issubset(args.keys()),
            f"Missing required: {expected_required - args.keys()}",
        )
        # Any extra key must be in MulesoftClientArgs schema.
        allowed = (
            MulesoftClientArgs.__required_keys__ | MulesoftClientArgs.__optional_keys__
        )
        unknown = args.keys() - allowed
        self.assertEqual(
            set(),
            unknown,
            f"connect_args has keys outside MulesoftClientArgs: {unknown}",
        )
