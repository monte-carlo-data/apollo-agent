"""
Tests for the resolve_informatica_session CTP transform.

Covers all three auth modes (V2 password, V3 password, JWT loginOAuth),
error paths, and credential safety.
"""

from unittest import TestCase
from unittest.mock import MagicMock, patch

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.transforms.registry import TransformRegistry
from apollo.integrations.ctp.transforms.resolve_informatica_session import (
    ResolveInformaticaSessionTransform,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_BASE_URL = "https://dm-us.informaticacloud.com"
_API_BASE_URL_V2 = "https://na1.informaticacloud.com"
_API_BASE_URL_V3 = "https://eu1.informaticacloud.com"

_V2_LOGIN_RESPONSE = {"serverUrl": _API_BASE_URL_V2, "icSessionId": "session-v2-abc"}
_V3_LOGIN_RESPONSE = {
    "products": [
        {"name": "Integration Cloud", "baseApiUrl": _API_BASE_URL_V3},
        {"name": "Other Service", "baseApiUrl": "https://other.example.com"},
    ],
    "userInfo": {"sessionId": "session-v3-xyz"},
}
_JWT_LOGIN_RESPONSE = {"serverUrl": _API_BASE_URL_V2, "icSessionId": "session-jwt-123"}


def _mock_post(body: dict) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = body
    resp.raise_for_status.return_value = None
    return resp


def _make_step(
    input_: dict, session_key: str = "session_id", base_url_key: str = "api_base_url"
) -> TransformStep:
    return TransformStep(
        type="resolve_informatica_session",
        input=input_,
        output={"session_id": session_key, "api_base_url": base_url_key},
    )


def _run(step: TransformStep, raw: dict) -> PipelineState:
    state = PipelineState(raw=raw)
    ResolveInformaticaSessionTransform().execute(step, state)
    return state


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestRegistration(TestCase):
    def test_registered(self):
        transform = TransformRegistry.get("resolve_informatica_session")
        self.assertIsInstance(transform, ResolveInformaticaSessionTransform)


# ---------------------------------------------------------------------------
# V2 password mode
# ---------------------------------------------------------------------------


class TestV2PasswordMode(TestCase):
    @patch("requests.post")
    def test_v2_login_extracts_server_url_and_session_id(self, mock_post):
        mock_post.return_value = _mock_post(_V2_LOGIN_RESPONSE)

        step = _make_step(
            {
                "username": "{{ raw.username }}",
                "password": "{{ raw.password }}",
                "informatica_auth": "v2",
                "base_url": _BASE_URL,
            }
        )
        state = _run(step, {"username": "svc_user", "password": "s3cr3t"})

        self.assertEqual("session-v2-abc", state.derived["session_id"])
        self.assertEqual(_API_BASE_URL_V2, state.derived["api_base_url"])

    @patch("requests.post")
    def test_v2_login_posts_to_v2_path(self, mock_post):
        mock_post.return_value = _mock_post(_V2_LOGIN_RESPONSE)

        step = _make_step(
            {
                "username": "{{ raw.username }}",
                "password": "{{ raw.password }}",
                "informatica_auth": "v2",
                "base_url": _BASE_URL,
            }
        )
        _run(step, {"username": "u", "password": "p"})

        login_url = mock_post.call_args[0][0]
        self.assertIn("/ma/api/v2/user/login", login_url)
        self.assertNotIn("loginOAuth", login_url)

    @patch("requests.post")
    def test_v2_login_sends_credentials_as_form_data(self, mock_post):
        mock_post.return_value = _mock_post(_V2_LOGIN_RESPONSE)

        step = _make_step(
            {
                "username": "{{ raw.username }}",
                "password": "{{ raw.password }}",
                "informatica_auth": "v2",
                "base_url": _BASE_URL,
            }
        )
        _run(step, {"username": "myuser", "password": "mypass"})

        call_kwargs = mock_post.call_args[1]
        self.assertEqual(
            {"username": "myuser", "password": "mypass"}, call_kwargs["data"]
        )


# ---------------------------------------------------------------------------
# V3 password mode
# ---------------------------------------------------------------------------


class TestV3PasswordMode(TestCase):
    @patch("requests.post")
    def test_v3_login_extracts_integration_cloud_base_url(self, mock_post):
        mock_post.return_value = _mock_post(_V3_LOGIN_RESPONSE)

        step = _make_step(
            {
                "username": "{{ raw.username }}",
                "password": "{{ raw.password }}",
                "informatica_auth": "v3",
                "base_url": _BASE_URL,
            }
        )
        state = _run(step, {"username": "svc_user", "password": "s3cr3t"})

        self.assertEqual("session-v3-xyz", state.derived["session_id"])
        self.assertEqual(_API_BASE_URL_V3, state.derived["api_base_url"])

    @patch("requests.post")
    def test_v3_login_posts_to_v3_path(self, mock_post):
        mock_post.return_value = _mock_post(_V3_LOGIN_RESPONSE)

        step = _make_step(
            {
                "username": "{{ raw.username }}",
                "password": "{{ raw.password }}",
                "informatica_auth": "v3",
                "base_url": _BASE_URL,
            }
        )
        _run(step, {"username": "u", "password": "p"})

        login_url = mock_post.call_args[0][0]
        self.assertIn("/saas/public/core/v3/login", login_url)

    @patch("requests.post")
    def test_missing_informatica_auth_defaults_to_v3(self, mock_post):
        """When informatica_auth is absent, V3 is used."""
        mock_post.return_value = _mock_post(_V3_LOGIN_RESPONSE)

        step = _make_step(
            {
                "username": "{{ raw.username }}",
                "password": "{{ raw.password }}",
            }
        )
        _run(step, {"username": "u", "password": "p"})

        login_url = mock_post.call_args[0][0]
        self.assertIn("/saas/public/core/v3/login", login_url)

    @patch("requests.post")
    def test_v3_login_sends_credentials_as_json(self, mock_post):
        """V3 endpoint requires application/json — must use json=, not data=."""
        mock_post.return_value = _mock_post(_V3_LOGIN_RESPONSE)

        step = _make_step(
            {
                "username": "{{ raw.username }}",
                "password": "{{ raw.password }}",
                "informatica_auth": "v3",
                "base_url": _BASE_URL,
            }
        )
        _run(step, {"username": "myuser", "password": "mypass"})

        call_kwargs = mock_post.call_args[1]
        self.assertEqual(
            {"username": "myuser", "password": "mypass"}, call_kwargs["json"]
        )
        self.assertNotIn("data", call_kwargs)

    @patch("requests.post")
    def test_v3_response_ignores_non_integration_cloud_products(self, mock_post):
        """Only the 'Integration Cloud' product's baseApiUrl is used."""
        mock_post.return_value = _mock_post(_V3_LOGIN_RESPONSE)

        step = _make_step(
            {
                "username": "{{ raw.username }}",
                "password": "{{ raw.password }}",
                "informatica_auth": "v3",
                "base_url": _BASE_URL,
            }
        )
        state = _run(step, {"username": "u", "password": "p"})

        self.assertNotEqual("https://other.example.com", state.derived["api_base_url"])
        self.assertEqual(_API_BASE_URL_V3, state.derived["api_base_url"])


# ---------------------------------------------------------------------------
# JWT mode
# ---------------------------------------------------------------------------


class TestJwtMode(TestCase):
    @patch("requests.post")
    def test_jwt_login_extracts_server_url_and_session_id(self, mock_post):
        mock_post.return_value = _mock_post(_JWT_LOGIN_RESPONSE)

        step = _make_step(
            {
                "jwt_token": "{{ raw.jwt_token }}",
                "org_id": "{{ raw.org_id }}",
                "base_url": _BASE_URL,
            }
        )
        state = _run(step, {"jwt_token": "eyJhb...", "org_id": "6KAbcd1EFG"})

        self.assertEqual("session-jwt-123", state.derived["session_id"])
        self.assertEqual(_API_BASE_URL_V2, state.derived["api_base_url"])

    @patch("requests.post")
    def test_jwt_login_posts_to_login_oauth_path(self, mock_post):
        mock_post.return_value = _mock_post(_JWT_LOGIN_RESPONSE)

        step = _make_step(
            {
                "jwt_token": "{{ raw.jwt_token }}",
                "org_id": "{{ raw.org_id }}",
                "base_url": _BASE_URL,
            }
        )
        _run(step, {"jwt_token": "eyJhb...", "org_id": "myorg"})

        login_url = mock_post.call_args[0][0]
        self.assertIn("/ma/api/v2/user/loginOAuth", login_url)

    @patch("requests.post")
    def test_jwt_login_sends_org_id_and_token_as_json(self, mock_post):
        mock_post.return_value = _mock_post(_JWT_LOGIN_RESPONSE)

        step = _make_step(
            {
                "jwt_token": "{{ raw.jwt_token }}",
                "org_id": "{{ raw.org_id }}",
                "base_url": _BASE_URL,
            }
        )
        _run(step, {"jwt_token": "my.jwt.token", "org_id": "myorg123"})

        call_kwargs = mock_post.call_args[1]
        self.assertEqual(
            {"orgId": "myorg123", "oauthToken": "my.jwt.token"}, call_kwargs["json"]
        )

    @patch("requests.post")
    def test_jwt_mode_uses_custom_base_url(self, mock_post):
        mock_post.return_value = _mock_post(_JWT_LOGIN_RESPONSE)
        custom_url = "https://eu1-dm.informaticacloud.com"

        step = _make_step(
            {
                "jwt_token": "{{ raw.jwt_token }}",
                "org_id": "{{ raw.org_id }}",
                "base_url": custom_url,
            }
        )
        _run(step, {"jwt_token": "t", "org_id": "org"})

        login_url = mock_post.call_args[0][0]
        self.assertTrue(login_url.startswith(custom_url))


# ---------------------------------------------------------------------------
# base_url handling
# ---------------------------------------------------------------------------


class TestBaseUrl(TestCase):
    @patch("requests.post")
    def test_custom_base_url_used_in_login_request(self, mock_post):
        mock_post.return_value = _mock_post(_V2_LOGIN_RESPONSE)
        custom = "https://dm-eu.informaticacloud.com"

        step = _make_step(
            {
                "username": "{{ raw.username }}",
                "password": "{{ raw.password }}",
                "informatica_auth": "v2",
                "base_url": custom,
            }
        )
        _run(step, {"username": "u", "password": "p"})

        login_url = mock_post.call_args[0][0]
        self.assertTrue(login_url.startswith(custom))

    @patch("requests.post")
    def test_missing_base_url_falls_back_to_default(self, mock_post):
        """When base_url is absent, the US pod default is used."""
        mock_post.return_value = _mock_post(_V2_LOGIN_RESPONSE)

        step = _make_step(
            {
                "username": "{{ raw.username }}",
                "password": "{{ raw.password }}",
                "informatica_auth": "v2",
            }
        )
        _run(step, {"username": "u", "password": "p"})

        login_url = mock_post.call_args[0][0]
        self.assertIn("dm-us.informaticacloud.com", login_url)


# ---------------------------------------------------------------------------
# Output key naming
# ---------------------------------------------------------------------------


class TestOutputKeys(TestCase):
    @patch("requests.post")
    def test_custom_output_keys_written_to_derived(self, mock_post):
        """Output key names in step.output control where values land in derived."""
        mock_post.return_value = _mock_post(_V2_LOGIN_RESPONSE)

        step = _make_step(
            {
                "username": "u",
                "password": "p",
                "informatica_auth": "v2",
                "base_url": _BASE_URL,
            },
            session_key="infa_session",
            base_url_key="infa_api_url",
        )
        state = _run(step, {})

        self.assertEqual("session-v2-abc", state.derived["infa_session"])
        self.assertEqual(_API_BASE_URL_V2, state.derived["infa_api_url"])


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestErrorPaths(TestCase):
    def test_no_credentials_raises(self):
        """Neither jwt_token nor username/password → CtpPipelineError."""
        step = _make_step({"base_url": _BASE_URL})
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {})
        self.assertIn("username", str(ctx.exception).lower())

    def test_jwt_without_org_id_raises(self):
        step = _make_step({"jwt_token": "tok", "base_url": _BASE_URL})
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {})
        self.assertIn("org_id", str(ctx.exception))

    def test_unsupported_informatica_auth_raises(self):
        step = _make_step(
            {
                "username": "u",
                "password": "p",
                "informatica_auth": "v99",
                "base_url": _BASE_URL,
            }
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {})
        self.assertIn("v99", str(ctx.exception))

    @patch("requests.post")
    def test_v2_response_missing_server_url_raises(self, mock_post):
        mock_post.return_value = _mock_post({"icSessionId": "s"})  # no serverUrl

        step = _make_step(
            {
                "username": "u",
                "password": "p",
                "informatica_auth": "v2",
                "base_url": _BASE_URL,
            }
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {})
        self.assertIn("serverUrl", str(ctx.exception))

    @patch("requests.post")
    def test_v3_response_missing_integration_cloud_raises(self, mock_post):
        mock_post.return_value = _mock_post(
            {
                "products": [{"name": "Other Service", "baseApiUrl": "https://x.com"}],
                "userInfo": {"sessionId": "s"},
            }
        )

        step = _make_step(
            {
                "username": "u",
                "password": "p",
                "informatica_auth": "v3",
                "base_url": _BASE_URL,
            }
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {})
        self.assertIn("baseApiUrl", str(ctx.exception))

    @patch("requests.post")
    def test_jwt_response_missing_session_id_raises(self, mock_post):
        mock_post.return_value = _mock_post(
            {"serverUrl": "https://x.com"}
        )  # no icSessionId

        step = _make_step(
            {
                "jwt_token": "tok",
                "org_id": "org",
                "base_url": _BASE_URL,
            }
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {})
        self.assertIn("icSessionId", str(ctx.exception))

    @patch("requests.post")
    def test_http_error_during_login_raises_ctp_error(self, mock_post):
        from requests import HTTPError

        mock_fail = MagicMock()
        mock_fail.status_code = 401
        mock_fail.raise_for_status.side_effect = HTTPError(response=mock_fail)
        mock_post.return_value = mock_fail

        step = _make_step(
            {
                "username": "u",
                "password": "p",
                "informatica_auth": "v2",
                "base_url": _BASE_URL,
            }
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {})
        error_msg = str(ctx.exception)
        self.assertIn("login failed", error_msg.lower())
        self.assertIn("HTTP 401", error_msg)


# ---------------------------------------------------------------------------
# Credential safety
# ---------------------------------------------------------------------------


class TestCredentialSafety(TestCase):
    """Credentials must not appear in CtpPipelineError messages."""

    _PASSWORD = "super_secret_password_xyz"
    _JWT = "eyJhbGciOiJSUzI1NiJ9.secret_payload.signature"

    @patch("requests.post")
    def test_password_not_leaked_in_login_failure_message(self, mock_post):
        from requests import HTTPError

        mock_fail = MagicMock()
        mock_fail.raise_for_status.side_effect = HTTPError(response=mock_fail)
        mock_post.return_value = mock_fail

        step = _make_step(
            {
                "username": "user",
                "password": self._PASSWORD,
                "informatica_auth": "v2",
                "base_url": _BASE_URL,
            }
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {})

        self.assertNotIn(self._PASSWORD, str(ctx.exception))

    @patch("requests.post")
    def test_jwt_token_not_leaked_in_login_failure_message(self, mock_post):
        from requests import HTTPError

        mock_fail = MagicMock()
        mock_fail.raise_for_status.side_effect = HTTPError(response=mock_fail)
        mock_post.return_value = mock_fail

        step = _make_step(
            {
                "jwt_token": self._JWT,
                "org_id": "org",
                "base_url": _BASE_URL,
            }
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {})

        self.assertNotIn(self._JWT, str(ctx.exception))
