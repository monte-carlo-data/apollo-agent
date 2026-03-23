# tests/ccp/test_databricks_ccp.py
from unittest import TestCase
from unittest.mock import MagicMock, patch

from apollo.integrations.ccp.defaults.databricks import (
    DATABRICKS_DEFAULT_CCP,
    DATABRICKS_REST_DEFAULT_CCP,
)
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


def _resolve_sql(credentials: dict) -> dict:
    return CcpPipeline().execute(DATABRICKS_DEFAULT_CCP, credentials)


def _resolve_rest(credentials: dict) -> dict:
    return CcpPipeline().execute(DATABRICKS_REST_DEFAULT_CCP, credentials)


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


class TestDatabricksSqlCcp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CcpRegistry.get("databricks"))

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

    @patch("apollo.integrations.ccp.transforms.resolve_databricks_oauth.Config")
    @patch(
        "apollo.integrations.ccp.transforms.resolve_databricks_oauth.oauth_service_principal"
    )
    def test_databricks_oauth_produces_credentials_provider(
        self, mock_provider, mock_config
    ):
        args = _resolve_sql(_SQL_OAUTH_CREDS)
        self.assertIn("credentials_provider", args)
        self.assertTrue(callable(args["credentials_provider"]))

    @patch("apollo.integrations.ccp.transforms.resolve_databricks_oauth.Config")
    @patch(
        "apollo.integrations.ccp.transforms.resolve_databricks_oauth.oauth_service_principal"
    )
    def test_databricks_oauth_client_id_not_in_connect_args(
        self, mock_provider, mock_config
    ):
        # client_id/secret must not reach sql.connect; proxy's _credentials_use_oauth
        # checks for them in connect_args — keeping them out prevents a double-OAuth call.
        args = _resolve_sql(_SQL_OAUTH_CREDS)
        self.assertNotIn("databricks_client_id", args)
        self.assertNotIn("databricks_client_secret", args)

    @patch("apollo.integrations.ccp.transforms.resolve_databricks_oauth.Config")
    @patch(
        "apollo.integrations.ccp.transforms.resolve_databricks_oauth.oauth_service_principal"
    )
    def test_databricks_oauth_no_access_token(self, mock_provider, mock_config):
        args = _resolve_sql(_SQL_OAUTH_CREDS)
        self.assertNotIn("access_token", args)

    # ── Azure-managed OAuth ───────────────────────────────────────────

    @patch("apollo.integrations.ccp.transforms.resolve_databricks_oauth.Config")
    @patch(
        "apollo.integrations.ccp.transforms.resolve_databricks_oauth.azure_service_principal"
    )
    def test_azure_oauth_uses_azure_service_principal(self, mock_provider, mock_config):
        args = _resolve_sql(_SQL_AZURE_OAUTH_CREDS)
        self.assertIn("credentials_provider", args)
        self.assertTrue(callable(args["credentials_provider"]))

    # ── Legacy passthrough ────────────────────────────────────────────

    def test_legacy_connect_args_passthrough(self):
        legacy = {
            "connect_args": {
                "server_hostname": "h",
                "http_path": "/p",
                "access_token": "t",
            }
        }
        self.assertEqual(legacy, CcpRegistry.resolve("databricks", legacy))


class TestDatabricksRestCcp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CcpRegistry.get("databricks-rest"))

    # ── PAT auth ──────────────────────────────────────────────────────

    def test_pat_fields(self):
        args = _resolve_rest(
            {
                "databricks_workspace_url": "https://workspace.azuredatabricks.net",
                "databricks_token": "dapi_pat_token",
            }
        )
        self.assertEqual(
            "https://workspace.azuredatabricks.net", args["databricks_workspace_url"]
        )
        self.assertEqual("dapi_pat_token", args["databricks_token"])

    # ── Databricks-managed OAuth ──────────────────────────────────────

    def test_databricks_oauth_fields(self):
        args = _resolve_rest(
            {
                "databricks_workspace_url": "https://workspace.azuredatabricks.net",
                "databricks_client_id": "client-id",
                "databricks_client_secret": "client-secret",
            }
        )
        self.assertEqual("client-id", args["databricks_client_id"])
        self.assertEqual("client-secret", args["databricks_client_secret"])
        self.assertNotIn("databricks_token", args)

    # ── Azure-managed OAuth ───────────────────────────────────────────

    def test_azure_oauth_fields(self):
        args = _resolve_rest(
            {
                "databricks_workspace_url": "https://workspace.azuredatabricks.net",
                "databricks_client_id": "client-id",
                "databricks_client_secret": "client-secret",
                "azure_tenant_id": "tenant-id",
                "azure_workspace_resource_id": "/subscriptions/sub/workspaces/ws",
            }
        )
        self.assertEqual("tenant-id", args["azure_tenant_id"])
        self.assertEqual(
            "/subscriptions/sub/workspaces/ws", args["azure_workspace_resource_id"]
        )

    def test_absent_optional_fields_omitted(self):
        args = _resolve_rest(
            {
                "databricks_workspace_url": "https://workspace.azuredatabricks.net",
                "databricks_token": "t",
            }
        )
        for field in (
            "databricks_client_id",
            "databricks_client_secret",
            "azure_tenant_id",
            "azure_workspace_resource_id",
        ):
            self.assertNotIn(field, args, f"expected {field!r} absent")

    # ── Legacy passthrough ────────────────────────────────────────────

    def test_legacy_connect_args_passthrough(self):
        legacy = {
            "connect_args": {
                "databricks_workspace_url": "https://w",
                "databricks_token": "t",
            }
        }
        self.assertEqual(legacy, CcpRegistry.resolve("databricks-rest", legacy))
