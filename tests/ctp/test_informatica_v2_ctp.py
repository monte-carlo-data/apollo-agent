from unittest import TestCase
from unittest.mock import MagicMock, patch

from apollo.agent.proxy_client_factory import ProxyClientFactory
from apollo.integrations.ctp.defaults.informatica_v2 import INFORMATICA_V2_DEFAULT_CTP
from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry

# Canned IDP token-endpoint and Informatica /loginOAuth responses
_IDP_TOKEN_RESPONSE = {
    "access_token": "jwt-issued-by-customer-idp",
    "expires_in": 3600,
    "token_type": "Bearer",
}
_INFORMATICA_LOGIN_OAUTH_RESPONSE = {
    "serverUrl": "https://na1.informaticacloud.com",
    "icSessionId": "session-v2-abc",
}

# Mirrors monolith's `OAuthConfiguration` GraphQL input — the same shape stored
# for Snowflake OAuth credentials.
_OAUTH_CONFIG_CLIENT_CREDENTIALS = {
    "client_id": "idp-client-id",
    "client_secret": "idp-client-secret",
    "access_token_endpoint": "https://idp.example.com/oauth2/token",
    "grant_type": "client_credentials",
    "scope": "informatica",
}

_OAUTH_MODE_CREDENTIALS = {
    "auth_mode": "oauth",
    "oauth": _OAUTH_CONFIG_CLIENT_CREDENTIALS,
    "org_id": "ORG-12345",
    "base_url": "https://dm-us.informaticacloud.com",
}

_PASSWORD_MODE_CREDENTIALS = {
    "auth_mode": "password",
    "username": "svc-user",
    "password": "svc-pass",
    "informatica_auth": "v3",
    "base_url": "https://dm-us.informaticacloud.com",
}

# Backwards-compat alias for tests written before the auth_mode discriminator.
_FLAT_CREDENTIALS = _OAUTH_MODE_CREDENTIALS


def _mock_post(body: dict) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = body
    resp.raise_for_status.return_value = None
    return resp


class TestInformaticaV2CtpRegistered(TestCase):
    def test_registered(self):
        """CtpRegistry.get('informatica-v2') must return a config — not None."""
        self.assertIsNotNone(CtpRegistry.get("informatica-v2"))


class TestInformaticaV2CtpPipeline(TestCase):
    """Verify the v2 pipeline chains oauth → resolve_informatica_session correctly."""

    @patch("requests.post")
    def test_oauth_credentials_produce_session_and_base_url(self, mock_post):
        """Flat OAuth credentials flow through both steps and produce connect_args."""
        mock_post.side_effect = [
            _mock_post(_IDP_TOKEN_RESPONSE),
            _mock_post(_INFORMATICA_LOGIN_OAUTH_RESPONSE),
        ]

        result = CtpPipeline().execute(
            INFORMATICA_V2_DEFAULT_CTP,
            _FLAT_CREDENTIALS,
        )

        self.assertEqual("session-v2-abc", result["session_id"])
        self.assertEqual("https://na1.informaticacloud.com", result["api_base_url"])
        # Raw credentials must not appear in connect_args
        self.assertNotIn("oauth", result)
        self.assertNotIn("org_id", result)

    @patch("requests.post")
    def test_first_call_is_idp_token_endpoint(self, mock_post):
        """Step 1 hits the IDP token URL with the grant_type and scope from raw.oauth."""
        mock_post.side_effect = [
            _mock_post(_IDP_TOKEN_RESPONSE),
            _mock_post(_INFORMATICA_LOGIN_OAUTH_RESPONSE),
        ]

        CtpPipeline().execute(INFORMATICA_V2_DEFAULT_CTP, _FLAT_CREDENTIALS)

        first_call_url = mock_post.call_args_list[0][0][0]
        first_call_data = mock_post.call_args_list[0][1]["data"]
        self.assertEqual("https://idp.example.com/oauth2/token", first_call_url)
        self.assertEqual("client_credentials", first_call_data["grant_type"])
        # `scope` from the raw.oauth blob is forwarded to the IDP request body.
        self.assertEqual("informatica", first_call_data["scope"])

    @patch("requests.post")
    def test_password_grant_carries_username_and_password(self, mock_post):
        """raw.oauth with grant_type=password forwards username/password to the IDP."""
        mock_post.side_effect = [
            _mock_post(_IDP_TOKEN_RESPONSE),
            _mock_post(_INFORMATICA_LOGIN_OAUTH_RESPONSE),
        ]

        creds = {
            **_FLAT_CREDENTIALS,
            "oauth": {
                "client_id": "cid",
                "client_secret": "csec",
                "access_token_endpoint": "https://idp.example.com/oauth2/token",
                "grant_type": "password",
                "username": "svc-user",
                "password": "svc-pass",
            },
        }
        CtpPipeline().execute(INFORMATICA_V2_DEFAULT_CTP, creds)

        first_call_data = mock_post.call_args_list[0][1]["data"]
        self.assertEqual("password", first_call_data["grant_type"])
        self.assertEqual("svc-user", first_call_data["username"])
        self.assertEqual("svc-pass", first_call_data["password"])

    @patch("requests.post")
    def test_second_call_is_informatica_login_oauth_with_jwt(self, mock_post):
        """Step 2 hits Informatica /loginOAuth with the JWT obtained in step 1."""
        mock_post.side_effect = [
            _mock_post(_IDP_TOKEN_RESPONSE),
            _mock_post(_INFORMATICA_LOGIN_OAUTH_RESPONSE),
        ]

        CtpPipeline().execute(INFORMATICA_V2_DEFAULT_CTP, _FLAT_CREDENTIALS)

        second_call_url = mock_post.call_args_list[1][0][0]
        # /ma/api/v2/user/loginOAuth path is exercised when jwt_token is provided
        self.assertIn("/ma/api/v2/user/loginOAuth", second_call_url)

    @patch("requests.post")
    def test_default_base_url_when_omitted(self, mock_post):
        """Omitting base_url falls back to the default Informatica POD URL."""
        mock_post.side_effect = [
            _mock_post(_IDP_TOKEN_RESPONSE),
            _mock_post(_INFORMATICA_LOGIN_OAUTH_RESPONSE),
        ]

        creds_without_base_url = {
            k: v for k, v in _FLAT_CREDENTIALS.items() if k != "base_url"
        }
        CtpPipeline().execute(INFORMATICA_V2_DEFAULT_CTP, creds_without_base_url)

        # Second call must still go to a default Informatica login URL
        second_call_url = mock_post.call_args_list[1][0][0]
        self.assertIn("informaticacloud.com/ma/api/v2/user/loginOAuth", second_call_url)


class TestInformaticaV2CtpPasswordMode(TestCase):
    """Verify password mode skips the OAuth step and goes straight to Informatica login."""

    _V3_LOGIN_RESPONSE = {
        "products": [
            {
                "name": "Integration Cloud",
                "baseApiUrl": "https://eu1.informaticacloud.com",
            },
        ],
        "userInfo": {"sessionId": "session-v3-xyz"},
    }

    @patch("requests.post")
    def test_password_mode_skips_oauth_step_and_does_v3_login(self, mock_post):
        mock_post.return_value = _mock_post(self._V3_LOGIN_RESPONSE)

        result = CtpPipeline().execute(
            INFORMATICA_V2_DEFAULT_CTP, _PASSWORD_MODE_CREDENTIALS
        )

        # Only one HTTP call — straight to Informatica's V3 login (no OAuth step).
        self.assertEqual(1, mock_post.call_count)
        login_url = mock_post.call_args_list[0][0][0]
        self.assertIn("/saas/public/core/v3/login", login_url)
        self.assertEqual("session-v3-xyz", result["session_id"])
        self.assertEqual("https://eu1.informaticacloud.com", result["api_base_url"])


class TestInformaticaV2CtpFailureSurfaces(TestCase):
    """Each external failure surface must raise CtpPipelineError without leaking creds."""

    @patch("requests.post")
    def test_idp_token_failure_raises(self, mock_post):
        """A 4xx from the IDP token endpoint surfaces as CtpPipelineError."""
        idp_failure = MagicMock()
        idp_failure.raise_for_status.side_effect = __import__("requests").HTTPError(
            response=MagicMock(status_code=401, text="invalid_client")
        )
        mock_post.return_value = idp_failure

        with self.assertRaises(CtpPipelineError):
            CtpPipeline().execute(INFORMATICA_V2_DEFAULT_CTP, _FLAT_CREDENTIALS)

    @patch("requests.post")
    def test_informatica_login_oauth_failure_raises(self, mock_post):
        """A 4xx from /loginOAuth (after IDP succeeded) surfaces as CtpPipelineError."""
        informatica_failure = MagicMock()
        informatica_failure.raise_for_status.side_effect = __import__(
            "requests"
        ).HTTPError(response=MagicMock(status_code=403, text="JWT not trusted"))
        mock_post.side_effect = [
            _mock_post(_IDP_TOKEN_RESPONSE),
            informatica_failure,
        ]

        with self.assertRaises(CtpPipelineError):
            CtpPipeline().execute(INFORMATICA_V2_DEFAULT_CTP, _FLAT_CREDENTIALS)


class TestInformaticaV2CtpFactoryResolution(TestCase):
    """Verify CTP runs before InformaticaProxyClient is instantiated for v2."""

    @patch("requests.post")
    def test_flat_credentials_resolved_to_connect_args_before_client_creation(
        self, mock_post
    ):
        mock_post.side_effect = [
            _mock_post(_IDP_TOKEN_RESPONSE),
            _mock_post(_INFORMATICA_LOGIN_OAUTH_RESPONSE),
        ]

        captured = {}

        def fake_factory(credentials, **kwargs):
            captured["credentials"] = credentials
            raise StopIteration  # bail before any further work

        with patch(
            "apollo.agent.proxy_client_factory._CLIENT_FACTORY_MAPPING",
            {"informatica-v2": fake_factory},
        ):
            with self.assertRaises(StopIteration):
                ProxyClientFactory._create_proxy_client(
                    "informatica-v2", _FLAT_CREDENTIALS, "local"
                )

        self.assertIn("connect_args", captured["credentials"])
        connect_args = captured["credentials"]["connect_args"]
        self.assertEqual("session-v2-abc", connect_args["session_id"])
        self.assertEqual(
            "https://na1.informaticacloud.com", connect_args["api_base_url"]
        )
        # Raw credentials must not reach the proxy client
        self.assertNotIn("client_id", connect_args)
        self.assertNotIn("client_secret", connect_args)
