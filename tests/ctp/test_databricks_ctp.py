# tests/ctp/test_databricks_ctp.py
from unittest import TestCase
from unittest.mock import MagicMock, patch

from apollo.integrations.ctp.defaults.databricks import (
    DATABRICKS_DEFAULT_CTP,
    DATABRICKS_REST_DEFAULT_CTP,
)
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


def _resolve_sql(credentials: dict) -> dict:
    return CtpPipeline().execute(DATABRICKS_DEFAULT_CTP, credentials)


def _resolve_rest(credentials: dict) -> dict:
    return CtpPipeline().execute(DATABRICKS_REST_DEFAULT_CTP, credentials)


_SQL_PAT_CREDS = {
    "server_hostname": "workspace.azuredatabricks.net",
    "http_path": "/sql/1.0/warehouses/abc123",
    "access_token": "dapi_pat_token",
}

_SQL_OAUTH_CREDS = {
    "server_hostname": "workspace.azuredatabricks.net",
    "http_path": "/sql/1.0/warehouses/abc123",
    "databricks_client_id": "client-id",
    "databricks_client_secret": "client-secret",
}

_SQL_AZURE_OAUTH_CREDS = {
    **_SQL_OAUTH_CREDS,
    "azure_tenant_id": "tenant-id",
    "azure_workspace_resource_id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Databricks/workspaces/ws",
}


class TestDatabricksSqlCtp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CtpRegistry.get("databricks"))

    # ── PAT auth ──────────────────────────────────────────────────────

    def test_pat_basic_fields(self):
        args = _resolve_sql(_SQL_PAT_CREDS)
        self.assertEqual("workspace.azuredatabricks.net", args["server_hostname"])
        self.assertEqual("/sql/1.0/warehouses/abc123", args["http_path"])
        self.assertEqual("dapi_pat_token", args["access_token"])

    def test_pat_no_credentials_provider(self):
        args = _resolve_sql(_SQL_PAT_CREDS)
        self.assertNotIn("credentials_provider", args)

    def test_use_arrow_defaults_to_false(self):
        args = _resolve_sql(_SQL_PAT_CREDS)
        self.assertFalse(args["_use_arrow_native_complex_types"])

    def test_user_agent_entry_absent_when_not_provided(self):
        args = _resolve_sql(_SQL_PAT_CREDS)
        self.assertNotIn("_user_agent_entry", args)

    def test_user_agent_entry_passed_through(self):
        args = _resolve_sql({**_SQL_PAT_CREDS, "_user_agent_entry": "Monte Carlo"})
        self.assertEqual("Monte Carlo", args["_user_agent_entry"])

    # ── Databricks-managed OAuth ──────────────────────────────────────

    @patch("apollo.integrations.ctp.transforms.resolve_databricks_oauth.Config")
    @patch(
        "apollo.integrations.ctp.transforms.resolve_databricks_oauth.oauth_service_principal"
    )
    def test_databricks_oauth_produces_credentials_provider(
        self, mock_provider, mock_config
    ):
        args = _resolve_sql(_SQL_OAUTH_CREDS)
        self.assertIn("credentials_provider", args)
        self.assertTrue(callable(args["credentials_provider"]))

    @patch("apollo.integrations.ctp.transforms.resolve_databricks_oauth.Config")
    @patch(
        "apollo.integrations.ctp.transforms.resolve_databricks_oauth.oauth_service_principal"
    )
    def test_databricks_oauth_client_id_not_in_connect_args(
        self, mock_provider, mock_config
    ):
        # client_id/secret must not reach sql.connect; proxy's _credentials_use_oauth
        # checks for them in connect_args — keeping them out prevents a double-OAuth call.
        args = _resolve_sql(_SQL_OAUTH_CREDS)
        self.assertNotIn("databricks_client_id", args)
        self.assertNotIn("databricks_client_secret", args)

    @patch("apollo.integrations.ctp.transforms.resolve_databricks_oauth.Config")
    @patch(
        "apollo.integrations.ctp.transforms.resolve_databricks_oauth.oauth_service_principal"
    )
    def test_databricks_oauth_no_access_token(self, mock_provider, mock_config):
        args = _resolve_sql(_SQL_OAUTH_CREDS)
        self.assertNotIn("access_token", args)

    # ── Azure-managed OAuth ───────────────────────────────────────────

    @patch("apollo.integrations.ctp.transforms.resolve_databricks_oauth.Config")
    @patch(
        "apollo.integrations.ctp.transforms.resolve_databricks_oauth.azure_service_principal"
    )
    def test_azure_oauth_uses_azure_service_principal(self, mock_provider, mock_config):
        args = _resolve_sql(_SQL_AZURE_OAUTH_CREDS)
        self.assertIn("credentials_provider", args)
        self.assertTrue(callable(args["credentials_provider"]))


_REST_BASE_CREDS = {"databricks_workspace_url": "https://workspace.azuredatabricks.net"}
_REST_PAT_CREDS = {**_REST_BASE_CREDS, "databricks_token": "dapi_pat_token"}
_REST_OAUTH_CREDS = {
    **_REST_BASE_CREDS,
    "databricks_client_id": "client-id",
    "databricks_client_secret": "client-secret",
}
_REST_AZURE_OAUTH_CREDS = {
    **_REST_OAUTH_CREDS,
    "azure_tenant_id": "tenant-id",
    "azure_workspace_resource_id": "/subscriptions/sub/workspaces/ws",
}


class TestDatabricksRestCtp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CtpRegistry.get("databricks-rest"))

    # ── PAT auth ──────────────────────────────────────────────────────

    def test_pat_resolves_token(self):
        args = _resolve_rest(_REST_PAT_CREDS)
        self.assertEqual(
            "https://workspace.azuredatabricks.net", args["databricks_workspace_url"]
        )
        self.assertEqual("dapi_pat_token", args["token"])

    def test_pat_no_raw_cred_fields_in_output(self):
        args = _resolve_rest(_REST_PAT_CREDS)
        for field in (
            "databricks_token",
            "databricks_client_id",
            "databricks_client_secret",
            "azure_tenant_id",
            "azure_workspace_resource_id",
        ):
            self.assertNotIn(
                field, args, f"expected {field!r} absent from connect_args"
            )

    # ── Databricks-managed OAuth ──────────────────────────────────────

    @patch(
        "apollo.integrations.ctp.transforms.resolve_databricks_token.oauth_service_principal"
    )
    @patch("apollo.integrations.ctp.transforms.resolve_databricks_token.Config")
    def test_databricks_oauth_resolves_token(self, mock_config, mock_provider):
        mock_provider.return_value = lambda: {"Authorization": "Bearer oauth-token-db"}
        args = _resolve_rest(_REST_OAUTH_CREDS)
        self.assertEqual("oauth-token-db", args["token"])

    @patch(
        "apollo.integrations.ctp.transforms.resolve_databricks_token.oauth_service_principal"
    )
    @patch("apollo.integrations.ctp.transforms.resolve_databricks_token.Config")
    def test_databricks_oauth_no_raw_cred_fields_in_output(
        self, mock_config, mock_provider
    ):
        mock_provider.return_value = lambda: {"Authorization": "Bearer oauth-token-db"}
        args = _resolve_rest(_REST_OAUTH_CREDS)
        for field in (
            "databricks_client_id",
            "databricks_client_secret",
        ):
            self.assertNotIn(
                field, args, f"expected {field!r} absent from connect_args"
            )

    # ── Azure-managed OAuth ───────────────────────────────────────────

    @patch(
        "apollo.integrations.ctp.transforms.resolve_databricks_token.azure_service_principal"
    )
    @patch("apollo.integrations.ctp.transforms.resolve_databricks_token.Config")
    def test_azure_oauth_resolves_token(self, mock_config, mock_provider):
        mock_provider.return_value = lambda: {
            "Authorization": "Bearer oauth-token-azure"
        }
        args = _resolve_rest(_REST_AZURE_OAUTH_CREDS)
        self.assertEqual("oauth-token-azure", args["token"])

    @patch(
        "apollo.integrations.ctp.transforms.resolve_databricks_token.azure_service_principal"
    )
    @patch("apollo.integrations.ctp.transforms.resolve_databricks_token.Config")
    def test_azure_oauth_uses_azure_service_principal(self, mock_config, mock_provider):
        mock_provider.return_value = lambda: {"Authorization": "Bearer t"}
        _resolve_rest(_REST_AZURE_OAUTH_CREDS)
        mock_config.assert_called_once_with(
            host="https://workspace.azuredatabricks.net",
            azure_client_id="client-id",
            azure_client_secret="client-secret",
            azure_tenant_id="tenant-id",
            azure_workspace_resource_id="/subscriptions/sub/workspaces/ws",
        )
        mock_provider.assert_called_once_with(mock_config.return_value)

    # ── OAuth priority over PAT ───────────────────────────────────────

    @patch(
        "apollo.integrations.ctp.transforms.resolve_databricks_token.oauth_service_principal"
    )
    @patch("apollo.integrations.ctp.transforms.resolve_databricks_token.Config")
    def test_oauth_takes_priority_over_stale_pat(self, mock_config, mock_provider):
        mock_provider.return_value = lambda: {"Authorization": "Bearer oauth-wins"}
        args = _resolve_rest({**_REST_OAUTH_CREDS, "databricks_token": "stale-pat"})
        self.assertEqual("oauth-wins", args["token"])
