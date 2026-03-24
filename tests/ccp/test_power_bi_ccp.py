# tests/ccp/test_power_bi_ccp.py
#
# The proxy client reads credentials flat and calls MSAL internally, then forwards
# token + auth_type="Bearer" to HttpProxyClient. Not registered until Phase 2 updates
# PowerBiProxyClient to read from connect_args.
# Tests use CcpPipeline().execute() directly and mock MSAL calls.
from unittest import TestCase
from unittest.mock import MagicMock, patch

from apollo.integrations.ccp.defaults.power_bi import POWERBI_DEFAULT_CCP
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


def _resolve(credentials: dict) -> dict:
    return CcpPipeline().execute(POWERBI_DEFAULT_CCP, credentials)


def _sp_creds(**kwargs) -> dict:
    return {
        "auth_mode": "service_principal",
        "client_id": "app-client-id",
        "client_secret": "app-secret",
        "tenant_id": "tenant-uuid",
        **kwargs,
    }


def _pu_creds(**kwargs) -> dict:
    return {
        "auth_mode": "primary_user",
        "client_id": "app-client-id",
        "tenant_id": "tenant-uuid",
        "username": "user@example.com",
        "password": "userpass",
        **kwargs,
    }


class TestPowerBiCcp(TestCase):
    def test_powerbi_not_registered(self):
        self.assertIsNone(CcpRegistry.get("power-bi"))

    # ── Service principal flow ────────────────────────────────────────

    def test_service_principal_token_stored(self):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {
            "access_token": "sp-token-xyz"
        }
        with patch("msal.ConfidentialClientApplication", return_value=mock_app):
            args = _resolve(_sp_creds())
        self.assertEqual("sp-token-xyz", args["token"])

    def test_auth_type_always_bearer(self):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {"access_token": "tok"}
        with patch("msal.ConfidentialClientApplication", return_value=mock_app):
            args = _resolve(_sp_creds())
        self.assertEqual("Bearer", args["auth_type"])

    # ── Primary user flow ─────────────────────────────────────────────

    def test_primary_user_token_stored(self):
        mock_app = MagicMock()
        mock_app.get_accounts.return_value = []
        mock_app.acquire_token_by_username_password.return_value = {
            "access_token": "pu-token-abc"
        }
        with patch("msal.PublicClientApplication", return_value=mock_app):
            args = _resolve(_pu_creds())
        self.assertEqual("pu-token-abc", args["token"])

    def test_primary_user_uses_cached_token(self):
        mock_app = MagicMock()
        mock_account = MagicMock()
        mock_app.get_accounts.return_value = [mock_account]
        mock_app.acquire_token_silent.return_value = {"access_token": "cached-token"}
        with patch("msal.PublicClientApplication", return_value=mock_app):
            args = _resolve(_pu_creds())
        self.assertEqual("cached-token", args["token"])
        mock_app.acquire_token_by_username_password.assert_not_called()

    # ── Error handling ────────────────────────────────────────────────

    def test_msal_error_raises(self):
        from apollo.integrations.ccp.errors import CcpPipelineError

        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {
            "error": "invalid_client",
            "error_description": "bad credentials",
        }
        with patch("msal.ConfidentialClientApplication", return_value=mock_app):
            with self.assertRaises(CcpPipelineError):
                _resolve(_sp_creds())
