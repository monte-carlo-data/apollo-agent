# tests/ctp/test_power_bi_ctp.py
from unittest import TestCase
from unittest.mock import MagicMock, patch

from apollo.integrations.ctp.defaults.power_bi import POWERBI_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(POWERBI_DEFAULT_CTP, credentials)


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


class TestPowerBiCtp(TestCase):
    def test_powerbi_registered(self):
        self.assertIsNotNone(CtpRegistry.get("power-bi"))

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
        from apollo.integrations.ctp.errors import CtpPipelineError

        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {
            "error": "invalid_client",
            "error_description": "bad credentials",
        }
        with patch("msal.ConfidentialClientApplication", return_value=mock_app):
            with self.assertRaises(CtpPipelineError):
                _resolve(_sp_creds())
