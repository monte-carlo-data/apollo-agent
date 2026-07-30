from unittest import TestCase
from unittest.mock import MagicMock, patch

from apollo.integrations.powerbi.msal_auth import (
    FABRIC_HOST,
    FABRIC_SCOPE,
    POWERBI_HOST,
    POWERBI_SCOPE,
    MsalAuthError,
    PowerBiTokenProvider,
    acquire_token,
)

_SP_ARGS = {
    "auth_mode": "service_principal",
    "client_id": "cid",
    "tenant_id": "tid",
    "client_secret": "csec",
}
_PU_ARGS = {
    "auth_mode": "primary_user",
    "client_id": "cid",
    "tenant_id": "tid",
    "username": "alice@example.com",
    "password": "hunter2",
}


class TestAcquireToken(TestCase):
    @patch("apollo.integrations.powerbi.msal_auth.msal.ConfidentialClientApplication")
    def test_service_principal_uses_given_scope(self, mock_app_cls):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {"access_token": "sp-tok"}
        mock_app_cls.return_value = mock_app

        token = acquire_token(
            auth_mode="service_principal",
            client_id="cid",
            tenant_id="tid",
            scopes=[FABRIC_SCOPE],
            client_secret="csec",
        )
        self.assertEqual("sp-tok", token)
        mock_app.acquire_token_for_client.assert_called_once_with(scopes=[FABRIC_SCOPE])

    @patch("apollo.integrations.powerbi.msal_auth.msal.PublicClientApplication")
    def test_primary_user_uses_given_scope(self, mock_app_cls):
        mock_app = MagicMock()
        mock_app.get_accounts.return_value = []
        mock_app.acquire_token_by_username_password.return_value = {
            "access_token": "pu-tok"
        }
        mock_app_cls.return_value = mock_app

        token = acquire_token(
            auth_mode="primary_user",
            client_id="cid",
            tenant_id="tid",
            scopes=[POWERBI_SCOPE],
            username="alice@example.com",
            password="hunter2",
        )
        self.assertEqual("pu-tok", token)
        mock_app.acquire_token_by_username_password.assert_called_once_with(
            username="alice@example.com", password="hunter2", scopes=[POWERBI_SCOPE]
        )

    def test_unsupported_auth_mode_raises(self):
        with self.assertRaises(MsalAuthError):
            acquire_token(
                auth_mode="magic",
                client_id="cid",
                tenant_id="tid",
                scopes=[POWERBI_SCOPE],
            )

    def test_missing_client_secret_raises(self):
        with self.assertRaises(MsalAuthError):
            acquire_token(
                auth_mode="service_principal",
                client_id="cid",
                tenant_id="tid",
                scopes=[POWERBI_SCOPE],
            )

    @patch("apollo.integrations.powerbi.msal_auth.msal.ConfidentialClientApplication")
    def test_error_response_raises(self, mock_app_cls):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {
            "error": "invalid_client",
            "error_description": "bad secret",
        }
        mock_app_cls.return_value = mock_app
        with self.assertRaises(MsalAuthError) as ctx:
            acquire_token(
                auth_mode="service_principal",
                client_id="cid",
                tenant_id="tid",
                scopes=[POWERBI_SCOPE],
                client_secret="csec",
            )
        self.assertIn("invalid_client", str(ctx.exception))


class TestPowerBiTokenProviderRawCreds(TestCase):
    @patch("apollo.integrations.powerbi.msal_auth.msal.ConfidentialClientApplication")
    def test_powerbi_host_gets_powerbi_scope(self, mock_app_cls):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {"access_token": "pbi"}
        mock_app_cls.return_value = mock_app

        provider = PowerBiTokenProvider(_SP_ARGS)
        self.assertEqual("pbi", provider.token_for_host(POWERBI_HOST))
        mock_app.acquire_token_for_client.assert_called_once_with(
            scopes=[POWERBI_SCOPE]
        )

    @patch("apollo.integrations.powerbi.msal_auth.msal.ConfidentialClientApplication")
    def test_fabric_host_gets_fabric_scope(self, mock_app_cls):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {"access_token": "fab"}
        mock_app_cls.return_value = mock_app

        provider = PowerBiTokenProvider(_SP_ARGS)
        self.assertEqual("fab", provider.token_for_host(FABRIC_HOST))
        mock_app.acquire_token_for_client.assert_called_once_with(scopes=[FABRIC_SCOPE])

    @patch("apollo.integrations.powerbi.msal_auth.msal.PublicClientApplication")
    def test_primary_user_fabric_host(self, mock_app_cls):
        mock_app = MagicMock()
        mock_app.get_accounts.return_value = []
        mock_app.acquire_token_by_username_password.return_value = {
            "access_token": "fab-pu"
        }
        mock_app_cls.return_value = mock_app

        provider = PowerBiTokenProvider(_PU_ARGS)
        self.assertEqual("fab-pu", provider.token_for_host(FABRIC_HOST))

    def test_other_host_gets_no_token(self):
        # No MSAL patch needed: an unmapped host must short-circuit before any minting.
        provider = PowerBiTokenProvider(_SP_ARGS)
        self.assertIsNone(provider.token_for_host("example.com"))

    def test_none_host_gets_no_token(self):
        # When the caller has no URL (urlparse(None).hostname is None), the `host or ""`
        # guard maps to the empty string -> no scope -> no token, no minting.
        provider = PowerBiTokenProvider(_SP_ARGS)
        self.assertIsNone(provider.token_for_host(None))

    def test_token_for_host_requires_normalized_host(self):
        # token_for_host does an exact dict lookup; case/port normalization is the
        # caller's responsibility (PowerBiProxyClient derives the host via
        # urlparse().hostname). A non-normalized host string therefore misses and
        # yields no token — pinning where normalization must happen.
        provider = PowerBiTokenProvider(_SP_ARGS)
        self.assertIsNone(provider.token_for_host("API.POWERBI.COM"))

    @patch("apollo.integrations.powerbi.msal_auth.msal.ConfidentialClientApplication")
    def test_token_cached_per_scope(self, mock_app_cls):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.side_effect = [
            {"access_token": "pbi"},
            {"access_token": "fab"},
        ]
        mock_app_cls.return_value = mock_app

        provider = PowerBiTokenProvider(_SP_ARGS)
        # Two PowerBI calls → minted once; one Fabric call → minted once.
        self.assertEqual("pbi", provider.token_for_host(POWERBI_HOST))
        self.assertEqual("pbi", provider.token_for_host(POWERBI_HOST))
        self.assertEqual("fab", provider.token_for_host(FABRIC_HOST))
        self.assertEqual(2, mock_app.acquire_token_for_client.call_count)

    @patch("apollo.integrations.powerbi.msal_auth.msal.ConfidentialClientApplication")
    def test_mint_failure_raises_clear_error(self, mock_app_cls):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {
            "error": "unauthorized_client",
            "error_description": "SP not authorized for Fabric",
        }
        mock_app_cls.return_value = mock_app

        provider = PowerBiTokenProvider(_SP_ARGS)
        with self.assertRaises(MsalAuthError):
            provider.token_for_host(FABRIC_HOST)


class TestPowerBiTokenProviderPreShaped(TestCase):
    def test_powerbi_host_served_preshaped_token(self):
        provider = PowerBiTokenProvider({"token": "pre-minted", "auth_type": "Bearer"})
        self.assertEqual("pre-minted", provider.token_for_host(POWERBI_HOST))

    def test_fabric_host_gets_none(self):
        provider = PowerBiTokenProvider({"token": "pre-minted", "auth_type": "Bearer"})
        self.assertIsNone(provider.token_for_host(FABRIC_HOST))

    def test_other_host_gets_none(self):
        provider = PowerBiTokenProvider({"token": "pre-minted", "auth_type": "Bearer"})
        self.assertIsNone(provider.token_for_host("example.com"))
