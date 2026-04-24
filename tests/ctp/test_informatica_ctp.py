from unittest import TestCase
from unittest.mock import MagicMock, patch

from apollo.agent.proxy_client_factory import ProxyClientFactory
from apollo.integrations.ctp.defaults.informatica import INFORMATICA_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry

# Canned Informatica login responses used across tests
_V2_LOGIN_RESPONSE = {
    "serverUrl": "https://na1.informaticacloud.com",
    "icSessionId": "session-v2-abc",
}
_V3_LOGIN_RESPONSE = {
    "products": [
        {"name": "Integration Cloud", "baseApiUrl": "https://eu1.informaticacloud.com"},
    ],
    "userInfo": {"sessionId": "session-v3-xyz"},
}


def _mock_post(body: dict) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = body
    resp.raise_for_status.return_value = None
    return resp


class TestInformaticaCtpRegistered(TestCase):
    def test_registered(self):
        """CtpRegistry.get("informatica") must return a config — not None."""
        self.assertIsNotNone(CtpRegistry.get("informatica"))


class TestInformaticaCtpPipeline(TestCase):
    """Verify the default CTP pipeline resolves credentials to session_id + api_base_url."""

    @patch("requests.post")
    def test_v2_credentials_produce_session_and_base_url(self, mock_post):
        """V2 credentials flow through resolve_informatica_session and produce connect_args."""
        mock_post.return_value = _mock_post(_V2_LOGIN_RESPONSE)

        result = CtpPipeline().execute(
            INFORMATICA_DEFAULT_CTP,
            {
                "username": "svc_user",
                "password": "s3cr3t",
                "informatica_auth": "v2",
                "base_url": "https://dm-us.informaticacloud.com",
            },
        )

        self.assertEqual("session-v2-abc", result["session_id"])
        self.assertEqual("https://na1.informaticacloud.com", result["api_base_url"])
        # Raw credentials must not appear in connect_args
        self.assertNotIn("username", result)
        self.assertNotIn("password", result)

    @patch("requests.post")
    def test_v3_credentials_produce_session_and_base_url(self, mock_post):
        """V3 credentials flow through resolve_informatica_session and produce connect_args."""
        mock_post.return_value = _mock_post(_V3_LOGIN_RESPONSE)

        result = CtpPipeline().execute(
            INFORMATICA_DEFAULT_CTP,
            {
                "username": "svc_user",
                "password": "s3cr3t",
                "informatica_auth": "v3",
            },
        )

        self.assertEqual("session-v3-xyz", result["session_id"])
        self.assertEqual("https://eu1.informaticacloud.com", result["api_base_url"])

    @patch("requests.post")
    def test_minimal_credentials_default_to_v3(self, mock_post):
        """Credentials without informatica_auth default to V3 login."""
        mock_post.return_value = _mock_post(_V3_LOGIN_RESPONSE)

        result = CtpPipeline().execute(
            INFORMATICA_DEFAULT_CTP,
            {"username": "svc_user", "password": "s3cr3t"},
        )

        login_url = mock_post.call_args[0][0]
        self.assertIn("/saas/public/core/v3/login", login_url)
        self.assertEqual("session-v3-xyz", result["session_id"])


class TestInformaticaCtpPreResolvedPath(TestCase):
    """Verify the when= guard skips login when session_id is already present."""

    @patch("requests.post")
    def test_pre_resolved_session_skips_login_step(self, mock_post):
        """When session_id is present in raw, the resolve_informatica_session step is skipped."""
        result = CtpPipeline().execute(
            INFORMATICA_DEFAULT_CTP,
            {
                "session_id": "pre-existing-session",
                "api_base_url": "https://na1.informaticacloud.com",
            },
        )

        mock_post.assert_not_called()
        self.assertEqual("pre-existing-session", result["session_id"])
        self.assertEqual("https://na1.informaticacloud.com", result["api_base_url"])


class TestInformaticaCtpFactoryResolution(TestCase):
    """Verify CTP pipeline runs before InformaticaProxyClient is instantiated."""

    @patch("requests.post")
    def test_flat_credentials_resolved_to_connect_args_before_client_creation(
        self, mock_post
    ):
        """Flat credentials are resolved by CTP to session_id+api_base_url before the factory."""
        mock_post.return_value = _mock_post(_V3_LOGIN_RESPONSE)

        flat = {"username": "svc_user", "password": "s3cr3t", "informatica_auth": "v3"}
        captured = {}

        def fake_factory(credentials, **kwargs):
            captured["credentials"] = credentials
            raise StopIteration  # bail before attempting real network calls

        with patch(
            "apollo.agent.proxy_client_factory._CLIENT_FACTORY_MAPPING",
            {"informatica": fake_factory},
        ):
            with self.assertRaises(StopIteration):
                ProxyClientFactory._create_proxy_client("informatica", flat, "local")

        self.assertIn("connect_args", captured["credentials"])
        connect_args = captured["credentials"]["connect_args"]
        self.assertEqual("session-v3-xyz", connect_args["session_id"])
        self.assertEqual(
            "https://eu1.informaticacloud.com", connect_args["api_base_url"]
        )
        # Raw credentials must not reach the proxy client
        self.assertNotIn("username", connect_args)
        self.assertNotIn("password", connect_args)
