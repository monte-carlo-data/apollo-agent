from unittest import TestCase
from unittest.mock import MagicMock, Mock, create_autospec, patch

from requests import HTTPError, Response

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_ERROR_ATTRS,
    ATTRIBUTE_NAME_ERROR_TYPE,
    ATTRIBUTE_NAME_RESULT,
)
from apollo.integrations.databricks.databricks_rest_proxy_client import (
    AuthenticationMode,
    DatabricksRestProxyClient,
)

_WORKSPACE_URL = "https://adb-123.azuredatabricks.net"
_PAT = "dapi-test-token"
_CLIENT_ID = "test-client-id"
_CLIENT_SECRET = "test-client-secret"
_AZURE_TENANT_ID = "test-tenant-id"
_AZURE_WORKSPACE_RESOURCE_ID = "test-resource-id"
_OAUTH_TOKEN = "mocked-oauth-access-token"

_PAT_CREDENTIALS = {
    "databricks_workspace_url": _WORKSPACE_URL,
    "databricks_token": _PAT,
}
_DATABRICKS_OAUTH_CREDENTIALS = {
    "databricks_workspace_url": _WORKSPACE_URL,
    "databricks_client_id": _CLIENT_ID,
    "databricks_client_secret": _CLIENT_SECRET,
}
_AZURE_OAUTH_CREDENTIALS = {
    "databricks_workspace_url": _WORKSPACE_URL,
    "databricks_client_id": _CLIENT_ID,
    "databricks_client_secret": _CLIENT_SECRET,
    "azure_tenant_id": _AZURE_TENANT_ID,
    "azure_workspace_resource_id": _AZURE_WORKSPACE_RESOURCE_ID,
}

_OPERATION = {
    "trace_id": "1234",
    "skip_cache": True,
    "commands": [
        {
            "method": "do_request",
            "kwargs": {
                "url": f"{_WORKSPACE_URL}/api/2.0/sql/warehouses/abc/start",
                "http_method": "POST",
            },
        }
    ],
}


class TestDatabricksRestProxyClientAuthMode(TestCase):
    """Unit tests for _authentication_mode — no network calls or client init needed."""

    def setUp(self) -> None:
        # Bypass __init__ so we can test the method in isolation
        self._client = DatabricksRestProxyClient.__new__(DatabricksRestProxyClient)

    def test_pat_mode(self):
        self.assertEqual(
            AuthenticationMode.TOKEN,
            self._client._authentication_mode(_PAT_CREDENTIALS),
        )

    def test_databricks_oauth_mode(self):
        self.assertEqual(
            AuthenticationMode.DATABRICKS_OAUTH,
            self._client._authentication_mode(_DATABRICKS_OAUTH_CREDENTIALS),
        )

    def test_azure_oauth_mode(self):
        self.assertEqual(
            AuthenticationMode.AZURE_OAUTH,
            self._client._authentication_mode(_AZURE_OAUTH_CREDENTIALS),
        )

    def test_oauth_takes_priority_over_pat(self):
        """OAuth keys take priority even when a stale PAT is also present."""
        mixed = {**_DATABRICKS_OAUTH_CREDENTIALS, "databricks_token": "stale-pat"}
        self.assertEqual(
            AuthenticationMode.DATABRICKS_OAUTH,
            self._client._authentication_mode(mixed),
        )

    def test_empty_credentials_raises(self):
        with self.assertRaises(RuntimeError):
            self._client._authentication_mode({})

    def test_missing_secret_raises(self):
        with self.assertRaises(RuntimeError):
            self._client._authentication_mode(
                {"databricks_client_id": _CLIENT_ID}  # no secret
            )


class TestDatabricksRestProxyClientRequests(TestCase):
    """Integration-style tests that exercise the full agent → proxy client path."""

    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())

    def _mock_http_success(self, mock_request, result: dict):
        mock_response = create_autospec(Response)
        mock_response.json.return_value = result
        mock_request.return_value = mock_response
        return mock_response

    # ------------------------------------------------------------------
    # PAT authentication
    # ------------------------------------------------------------------

    @patch("requests.request")
    def test_do_request_with_pat(self, mock_request):
        self._mock_http_success(mock_request, {"result": "ok"})

        response = self._agent.execute_operation(
            "databricks-rest", "start_warehouse", _OPERATION, _PAT_CREDENTIALS
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertIn(ATTRIBUTE_NAME_RESULT, response.result)
        self.assertEqual(
            f"Bearer {_PAT}",
            mock_request.call_args[1]["headers"]["Authorization"],
        )

    @patch("requests.request")
    def test_do_request_with_connect_args_format(self, mock_request):
        """Credentials wrapped under 'connect_args' are unpacked correctly."""
        self._mock_http_success(mock_request, {"result": "ok"})

        response = self._agent.execute_operation(
            "databricks-rest",
            "start_warehouse",
            _OPERATION,
            {"connect_args": _PAT_CREDENTIALS},
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual(
            f"Bearer {_PAT}",
            mock_request.call_args[1]["headers"]["Authorization"],
        )

    # ------------------------------------------------------------------
    # Databricks OAuth authentication
    # ------------------------------------------------------------------

    @patch("requests.request")
    @patch(
        "apollo.integrations.databricks.databricks_rest_proxy_client.oauth_service_principal"
    )
    @patch("apollo.integrations.databricks.databricks_rest_proxy_client.Config")
    def test_do_request_with_databricks_oauth(
        self, mock_config_cls, mock_oauth_provider, mock_request
    ):
        # Config.__init__ probes the OIDC endpoint; mock the class to avoid network calls.
        self._mock_http_success(mock_request, {"result": "ok"})
        mock_oauth_provider.return_value = Mock(
            return_value={"Authorization": f"Bearer {_OAUTH_TOKEN}"}
        )

        response = self._agent.execute_operation(
            "databricks-rest",
            "start_warehouse",
            _OPERATION,
            _DATABRICKS_OAUTH_CREDENTIALS,
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        mock_config_cls.assert_called_once_with(
            host=_WORKSPACE_URL,
            client_id=_CLIENT_ID,
            client_secret=_CLIENT_SECRET,
        )
        mock_oauth_provider.assert_called_once_with(mock_config_cls.return_value)
        self.assertEqual(
            f"Bearer {_OAUTH_TOKEN}",
            mock_request.call_args[1]["headers"]["Authorization"],
        )

    # ------------------------------------------------------------------
    # Azure OAuth authentication
    # ------------------------------------------------------------------

    @patch("requests.request")
    @patch(
        "apollo.integrations.databricks.databricks_rest_proxy_client.azure_service_principal"
    )
    @patch("apollo.integrations.databricks.databricks_rest_proxy_client.Config")
    def test_do_request_with_azure_oauth(
        self, mock_config_cls, mock_azure_provider, mock_request
    ):
        # Config.__init__ probes the OIDC endpoint; mock the class to avoid network calls.
        self._mock_http_success(mock_request, {"result": "ok"})
        mock_azure_provider.return_value = Mock(
            return_value={"Authorization": f"Bearer {_OAUTH_TOKEN}"}
        )

        response = self._agent.execute_operation(
            "databricks-rest",
            "start_warehouse",
            _OPERATION,
            _AZURE_OAUTH_CREDENTIALS,
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        mock_config_cls.assert_called_once_with(
            host=_WORKSPACE_URL,
            azure_client_id=_CLIENT_ID,
            azure_client_secret=_CLIENT_SECRET,
            azure_tenant_id=_AZURE_TENANT_ID,
            azure_workspace_resource_id=_AZURE_WORKSPACE_RESOURCE_ID,
        )
        mock_azure_provider.assert_called_once_with(mock_config_cls.return_value)
        self.assertEqual(
            f"Bearer {_OAUTH_TOKEN}",
            mock_request.call_args[1]["headers"]["Authorization"],
        )

    # ------------------------------------------------------------------
    # Error cases
    # ------------------------------------------------------------------

    def test_no_supported_credentials_surfaces_error(self):
        """Missing token/OAuth keys → RuntimeError is surfaced in the agent response."""
        response = self._agent.execute_operation(
            "databricks-rest",
            "start_warehouse",
            _OPERATION,
            {"databricks_workspace_url": _WORKSPACE_URL},
        )

        self.assertIsNotNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertIn(
            "No supported credentials mode found",
            response.result.get(ATTRIBUTE_NAME_ERROR),
        )

    @patch("requests.request")
    def test_http_error_is_surfaced(self, mock_request):
        """HTTP 4xx errors are propagated as structured error attributes."""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.text = "Forbidden"
        mock_response.reason = "Forbidden"
        mock_response.raise_for_status.side_effect = HTTPError(response=mock_response)
        mock_request.return_value = mock_response

        response = self._agent.execute_operation(
            "databricks-rest", "start_warehouse", _OPERATION, _PAT_CREDENTIALS
        )

        self.assertIsNotNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual("HTTPError", response.result.get(ATTRIBUTE_NAME_ERROR_TYPE))
        self.assertEqual(
            {"status_code": 403, "reason": "Forbidden"},
            response.result.get(ATTRIBUTE_NAME_ERROR_ATTRS),
        )
