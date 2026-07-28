from unittest import TestCase
from unittest.mock import create_autospec, patch

from requests import Response

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.proxy_client_factory import ProxyClientFactory
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_RESULT,
)
from apollo.integrations.powerbi.msal_auth import FABRIC_SCOPE, POWERBI_SCOPE

_MSAL_SP = "apollo.integrations.powerbi.msal_auth.msal.ConfidentialClientApplication"
_MSAL_PU = "apollo.integrations.powerbi.msal_auth.msal.PublicClientApplication"

# Legacy pre-shaped path: MSAL token already minted by the DC, delivered via connect_args.
_PRESHAPED_CREDENTIALS = {
    "connect_args": {
        "token": "test-bearer-token",
        "auth_type": "Bearer",
    }
}
# Raw-creds path (the normal agent path): the agent mints the token, scoped by request host.
_SP_CREDENTIALS = {
    "auth_mode": "service_principal",
    "client_id": "cid",
    "tenant_id": "tid",
    "client_secret": "csec",
}
_PU_CREDENTIALS = {
    "auth_mode": "primary_user",
    "client_id": "cid",
    "tenant_id": "tid",
    "username": "user@example.com",
    "password": "userpass",
}

_POWERBI_URL = "https://api.powerbi.com/v1.0/myorg/admin/dataflows"
_FABRIC_URL = "https://api.fabric.microsoft.com/v1/workspaces"
_OTHER_URL = "https://example.com/path"


def _operation(url: str) -> dict:
    return {
        "trace_id": "1234",
        "commands": [
            {
                "method": "do_request",
                "kwargs": {
                    "url": url,
                    "http_method": "GET",
                    "payload": {},
                    "additional_headers": {"Content-Type": "application/json"},
                },
            }
        ],
    }


class TestPowerBiClient(TestCase):
    def setUp(self) -> None:
        # The factory caches clients (and their token providers) by credential hash; clear it so
        # each test mints against its own MSAL mock rather than a provider warmed by a prior test.
        ProxyClientFactory._clients_cache.clear()
        self._agent = Agent(LoggingUtils())

    def _run(self, url: str, credentials: dict):
        mock_response = create_autospec(Response)
        mock_response.json.return_value = {"ok": True}
        with patch("requests.request", return_value=mock_response) as mock_request:
            response = self._agent.execute_operation(
                connection_type="power-bi",
                operation_name="do_request",
                operation_dict=_operation(url),
                credentials=credentials,
            )
        return mock_request, response

    def _assert_ok(self, response):
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        self.assertEqual({"ok": True}, response.result.get(ATTRIBUTE_NAME_RESULT))

    # ── Legacy pre-shaped token: Power BI-scoped, served only for api.powerbi.com ──

    def test_preshaped_token_attached_for_powerbi_host(self):
        mock_request, response = self._run(_POWERBI_URL, _PRESHAPED_CREDENTIALS)
        mock_request.assert_called_with(
            "GET",
            _POWERBI_URL,
            headers={
                "Authorization": "Bearer test-bearer-token",
                "Content-Type": "application/json",
            },
        )
        self._assert_ok(response)

    def test_preshaped_token_not_attached_for_other_host(self):
        mock_request, _ = self._run(_OTHER_URL, _PRESHAPED_CREDENTIALS)
        mock_request.assert_called_with(
            "GET",
            _OTHER_URL,
            headers={"Content-Type": "application/json"},
        )

    # ── Raw creds, service principal: scope chosen by host ─────────────────────

    @patch(_MSAL_SP)
    def test_service_principal_powerbi_host_gets_powerbi_scope(self, mock_app_cls):
        mock_app = mock_app_cls.return_value
        mock_app.acquire_token_for_client.return_value = {"access_token": "pbi-tok"}

        mock_request, response = self._run(_POWERBI_URL, _SP_CREDENTIALS)

        mock_app.acquire_token_for_client.assert_called_once_with(
            scopes=[POWERBI_SCOPE]
        )
        mock_request.assert_called_with(
            "GET",
            _POWERBI_URL,
            headers={
                "Authorization": "Bearer pbi-tok",
                "Content-Type": "application/json",
            },
        )
        self._assert_ok(response)

    @patch(_MSAL_SP)
    def test_service_principal_fabric_host_gets_fabric_scope(self, mock_app_cls):
        mock_app = mock_app_cls.return_value
        mock_app.acquire_token_for_client.return_value = {"access_token": "fab-tok"}

        mock_request, response = self._run(_FABRIC_URL, _SP_CREDENTIALS)

        mock_app.acquire_token_for_client.assert_called_once_with(scopes=[FABRIC_SCOPE])
        mock_request.assert_called_with(
            "GET",
            _FABRIC_URL,
            headers={
                "Authorization": "Bearer fab-tok",
                "Content-Type": "application/json",
            },
        )
        self._assert_ok(response)

    @patch(_MSAL_SP)
    def test_service_principal_other_host_gets_no_token(self, mock_app_cls):
        mock_app = mock_app_cls.return_value
        mock_app.acquire_token_for_client.return_value = {"access_token": "unused"}

        mock_request, _ = self._run(_OTHER_URL, _SP_CREDENTIALS)

        # Unmapped host → no scope → no minting and no Authorization header.
        mock_app.acquire_token_for_client.assert_not_called()
        mock_request.assert_called_with(
            "GET",
            _OTHER_URL,
            headers={"Content-Type": "application/json"},
        )

    # ── Raw creds, primary user: scope chosen by host ─────────────────────────

    @patch(_MSAL_PU)
    def test_primary_user_fabric_host_gets_fabric_scope(self, mock_app_cls):
        mock_app = mock_app_cls.return_value
        mock_app.get_accounts.return_value = []
        mock_app.acquire_token_by_username_password.return_value = {
            "access_token": "fab-pu-tok"
        }

        mock_request, response = self._run(_FABRIC_URL, _PU_CREDENTIALS)

        mock_app.acquire_token_by_username_password.assert_called_once_with(
            username="user@example.com", password="userpass", scopes=[FABRIC_SCOPE]
        )
        mock_request.assert_called_with(
            "GET",
            _FABRIC_URL,
            headers={
                "Authorization": "Bearer fab-pu-tok",
                "Content-Type": "application/json",
            },
        )
        self._assert_ok(response)

    @patch(_MSAL_PU)
    def test_primary_user_powerbi_host_gets_powerbi_scope(self, mock_app_cls):
        mock_app = mock_app_cls.return_value
        mock_app.get_accounts.return_value = []
        mock_app.acquire_token_by_username_password.return_value = {
            "access_token": "pbi-pu-tok"
        }

        mock_request, response = self._run(_POWERBI_URL, _PU_CREDENTIALS)

        mock_app.acquire_token_by_username_password.assert_called_once_with(
            username="user@example.com", password="userpass", scopes=[POWERBI_SCOPE]
        )
        mock_request.assert_called_with(
            "GET",
            _POWERBI_URL,
            headers={
                "Authorization": "Bearer pbi-pu-tok",
                "Content-Type": "application/json",
            },
        )
        self._assert_ok(response)
