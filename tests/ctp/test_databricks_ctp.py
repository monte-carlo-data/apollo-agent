# tests/ctp/test_databricks_ctp.py
from unittest import TestCase
from unittest.mock import MagicMock, patch

from apollo.integrations.ctp.defaults.databricks import (
    DATABRICKS_DEFAULT_CTP,
    DATABRICKS_REST_DEFAULT_CTP,
)
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry
from apollo.integrations.ctp.transforms import _oauth_cache as _oauth_cache_mod


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
    def setUp(self):
        # Clear the OAuth HeaderFactory cache between tests so each test sees
        # a fresh cache miss and exercises the build path under its own mocks.
        _oauth_cache_mod._reset_for_tests()

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

    # ── server_hostname fallback ──────────────────────────────────────

    def test_server_hostname_falls_back_to_workspace_url(self):
        creds = {k: v for k, v in _SQL_PAT_CREDS.items() if k != "server_hostname"}
        creds["databricks_workspace_url"] = "https://workspace.azuredatabricks.net"
        args = _resolve_sql(creds)
        self.assertEqual("workspace.azuredatabricks.net", args["server_hostname"])

    def test_server_hostname_fallback_strips_trailing_slash(self):
        creds = {k: v for k, v in _SQL_PAT_CREDS.items() if k != "server_hostname"}
        creds["databricks_workspace_url"] = "https://workspace.azuredatabricks.net/"
        args = _resolve_sql(creds)
        self.assertEqual("workspace.azuredatabricks.net", args["server_hostname"])

    def test_server_hostname_takes_priority_over_workspace_url(self):
        creds = {
            **_SQL_PAT_CREDS,
            "databricks_workspace_url": "https://other.azuredatabricks.net",
        }
        args = _resolve_sql(creds)
        self.assertEqual("workspace.azuredatabricks.net", args["server_hostname"])

    # ── Databricks-managed OAuth ──────────────────────────────────────

    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.oauth_service_principal")
    def test_databricks_oauth_produces_credentials_provider(
        self, mock_provider, mock_config
    ):
        args = _resolve_sql(_SQL_OAUTH_CREDS)
        self.assertIn("credentials_provider", args)
        self.assertTrue(callable(args["credentials_provider"]))

    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.oauth_service_principal")
    def test_databricks_oauth_client_id_not_in_connect_args(
        self, mock_provider, mock_config
    ):
        # client_id/secret must not reach sql.connect; proxy's _credentials_use_oauth
        # checks for them in connect_args — keeping them out prevents a double-OAuth call.
        args = _resolve_sql(_SQL_OAUTH_CREDS)
        self.assertNotIn("databricks_client_id", args)
        self.assertNotIn("databricks_client_secret", args)

    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.oauth_service_principal")
    def test_databricks_oauth_no_access_token(self, mock_provider, mock_config):
        args = _resolve_sql(_SQL_OAUTH_CREDS)
        self.assertNotIn("access_token", args)

    # ── Azure-managed OAuth ───────────────────────────────────────────

    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.azure_service_principal")
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
    def setUp(self):
        # Clear the OAuth HeaderFactory cache between tests so each test sees
        # a fresh cache miss and exercises the build path under its own mocks.
        _oauth_cache_mod._reset_for_tests()

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

    @patch("apollo.integrations.ctp.transforms._oauth_cache.oauth_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    def test_databricks_oauth_resolves_token(self, mock_config, mock_provider):
        mock_provider.return_value = lambda: {"Authorization": "Bearer oauth-token-db"}
        args = _resolve_rest(_REST_OAUTH_CREDS)
        self.assertEqual("oauth-token-db", args["token"])

    @patch("apollo.integrations.ctp.transforms._oauth_cache.oauth_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
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

    # ── DC pre-shaped path ────────────────────────────────────────────

    def test_dc_shaped_token_passed_through(self):
        # DC pre-shapes credentials as {"token": "...", "databricks_workspace_url": "..."}.
        # The resolve_databricks_token step is skipped (no raw auth keys present);
        # the mapper reads token directly from raw.
        dc_shaped = {
            "databricks_workspace_url": "https://workspace.azuredatabricks.net",
            "token": "already-resolved-token",
        }
        args = _resolve_rest(dc_shaped)
        self.assertEqual("already-resolved-token", args["token"])
        self.assertEqual(
            "https://workspace.azuredatabricks.net", args["databricks_workspace_url"]
        )

    # ── Azure-managed OAuth ───────────────────────────────────────────

    @patch("apollo.integrations.ctp.transforms._oauth_cache.azure_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    def test_azure_oauth_resolves_token(self, mock_config, mock_provider):
        mock_provider.return_value = lambda: {
            "Authorization": "Bearer oauth-token-azure"
        }
        args = _resolve_rest(_REST_AZURE_OAUTH_CREDS)
        self.assertEqual("oauth-token-azure", args["token"])

    @patch("apollo.integrations.ctp.transforms._oauth_cache.azure_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
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

    @patch("apollo.integrations.ctp.transforms._oauth_cache.oauth_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    def test_oauth_takes_priority_over_stale_pat(self, mock_config, mock_provider):
        mock_provider.return_value = lambda: {"Authorization": "Bearer oauth-wins"}
        args = _resolve_rest({**_REST_OAUTH_CREDS, "databricks_token": "stale-pat"})
        self.assertEqual("oauth-wins", args["token"])


class TestOAuthCache(TestCase):
    """
    Tests for the shared OAuth HeaderFactory cache (``_oauth_cache``).

    The cache lives at module scope, so each test must reset it via setUp to
    avoid leakage from earlier tests (including those in the classes above).
    """

    def setUp(self):
        _oauth_cache_mod._reset_for_tests()

    # ── cache key shape ───────────────────────────────────────────────

    def test_secret_not_in_cache_key(self):
        key = _oauth_cache_mod._oauth_cache_key(
            "workspace.azuredatabricks.net",
            "client-id",
            "super-secret-do-not-leak",
            None,
            None,
        )
        self.assertNotIn("super-secret-do-not-leak", key)
        self.assertEqual(len(key), 64)  # sha256 hex digest

    def test_same_credentials_produce_same_key(self):
        k1 = _oauth_cache_mod._oauth_cache_key("w", "id", "sec", "t", "r")
        k2 = _oauth_cache_mod._oauth_cache_key("w", "id", "sec", "t", "r")
        self.assertEqual(k1, k2)

    def test_different_client_id_produces_different_key(self):
        k1 = _oauth_cache_mod._oauth_cache_key("w", "id-1", "sec", None, None)
        k2 = _oauth_cache_mod._oauth_cache_key("w", "id-2", "sec", None, None)
        self.assertNotEqual(k1, k2)

    def test_different_secret_produces_different_key(self):
        k1 = _oauth_cache_mod._oauth_cache_key("w", "id", "sec-1", None, None)
        k2 = _oauth_cache_mod._oauth_cache_key("w", "id", "sec-2", None, None)
        self.assertNotEqual(k1, k2)

    def test_different_workspace_produces_different_key(self):
        k1 = _oauth_cache_mod._oauth_cache_key("w1", "id", "sec", None, None)
        k2 = _oauth_cache_mod._oauth_cache_key("w2", "id", "sec", None, None)
        self.assertNotEqual(k1, k2)

    def test_azure_fields_change_key(self):
        databricks = _oauth_cache_mod._oauth_cache_key("w", "id", "sec", None, None)
        azure = _oauth_cache_mod._oauth_cache_key("w", "id", "sec", "t", "r")
        self.assertNotEqual(databricks, azure)

    # ── cache hit/miss behavior ───────────────────────────────────────

    @patch("apollo.integrations.ctp.transforms._oauth_cache.oauth_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    def test_same_credentials_hit_cache(self, mock_config, mock_provider):
        mock_provider.return_value = lambda: {"Authorization": "Bearer tok"}
        _oauth_cache_mod.cached_header_factory("w", "id", "sec", None, None)
        _oauth_cache_mod.cached_header_factory("w", "id", "sec", None, None)
        # Factory built once; second call serves from cache.
        mock_provider.assert_called_once()
        info = _oauth_cache_mod.cached_header_factory.cache_info()
        self.assertEqual(1, info.hits)
        self.assertEqual(1, info.misses)

    @patch("apollo.integrations.ctp.transforms._oauth_cache.oauth_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    def test_different_credentials_miss_cache(self, mock_config, mock_provider):
        mock_provider.return_value = lambda: {"Authorization": "Bearer tok"}
        _oauth_cache_mod.cached_header_factory("w", "id-1", "sec", None, None)
        _oauth_cache_mod.cached_header_factory("w", "id-2", "sec", None, None)
        self.assertEqual(2, mock_provider.call_count)
        info = _oauth_cache_mod.cached_header_factory.cache_info()
        self.assertEqual(0, info.hits)
        self.assertEqual(2, info.misses)

    @patch("apollo.integrations.ctp.transforms._oauth_cache.oauth_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    def test_rotated_secret_misses_cache(self, mock_config, mock_provider):
        mock_provider.return_value = lambda: {"Authorization": "Bearer tok"}
        _oauth_cache_mod.cached_header_factory("w", "id", "old-sec", None, None)
        _oauth_cache_mod.cached_header_factory("w", "id", "new-sec", None, None)
        self.assertEqual(2, mock_provider.call_count)

    # ── provider selection ────────────────────────────────────────────

    @patch("apollo.integrations.ctp.transforms._oauth_cache.azure_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.oauth_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    def test_databricks_oauth_uses_databricks_service_principal(
        self, mock_config, mock_oauth, mock_azure
    ):
        mock_oauth.return_value = lambda: {"Authorization": "Bearer t"}
        _oauth_cache_mod.cached_header_factory("w", "id", "sec", None, None)
        mock_oauth.assert_called_once()
        mock_azure.assert_not_called()

    @patch("apollo.integrations.ctp.transforms._oauth_cache.azure_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.oauth_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    def test_azure_oauth_uses_azure_service_principal(
        self, mock_config, mock_oauth, mock_azure
    ):
        mock_azure.return_value = lambda: {"Authorization": "Bearer t"}
        _oauth_cache_mod.cached_header_factory("w", "id", "sec", "tenant", "rsrc")
        mock_azure.assert_called_once()
        mock_oauth.assert_not_called()

    # ── LRU eviction ──────────────────────────────────────────────────

    @patch("apollo.integrations.ctp.transforms._oauth_cache.oauth_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    def test_lru_eviction_caps_cache_size(self, mock_config, mock_provider):
        mock_provider.return_value = lambda: {"Authorization": "Bearer t"}
        max_size = _oauth_cache_mod.cached_header_factory.cache_info().maxsize
        # Insert max_size + 5 distinct entries; cache should never exceed max.
        for i in range(max_size + 5):
            _oauth_cache_mod.cached_header_factory(
                "w", f"client-{i}", "sec", None, None
            )
        info = _oauth_cache_mod.cached_header_factory.cache_info()
        self.assertEqual(max_size, info.currsize)
        self.assertEqual(max_size + 5, info.misses)

    # ── REST transform uses the cache ─────────────────────────────────

    @patch("apollo.integrations.ctp.transforms._oauth_cache.oauth_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    def test_rest_transform_reuses_factory_across_calls(
        self, mock_config, mock_provider
    ):
        mock_provider.return_value = lambda: {"Authorization": "Bearer t"}
        _resolve_rest(_REST_OAUTH_CREDS)
        _resolve_rest(_REST_OAUTH_CREDS)
        # The transform should hit the cache on the second call.
        mock_provider.assert_called_once()

    def test_rest_pat_path_does_not_touch_cache(self):
        # PAT-only credentials should not invoke the OAuth path at all.
        _resolve_rest(_REST_PAT_CREDS)
        info = _oauth_cache_mod.cached_header_factory.cache_info()
        self.assertEqual(0, info.currsize)
        self.assertEqual(0, info.hits)
        self.assertEqual(0, info.misses)

    # ── SQL transform uses the cache ──────────────────────────────────

    @patch("apollo.integrations.ctp.transforms._oauth_cache.oauth_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    def test_sql_transform_reuses_factory_across_calls(
        self, mock_config, mock_provider
    ):
        mock_provider.return_value = lambda: {"Authorization": "Bearer t"}
        _resolve_sql(_SQL_OAUTH_CREDS)
        _resolve_sql(_SQL_OAUTH_CREDS)
        mock_provider.assert_called_once()

    @patch("apollo.integrations.ctp.transforms._oauth_cache.oauth_service_principal")
    @patch("apollo.integrations.ctp.transforms._oauth_cache.Config")
    def test_sql_credentials_provider_returns_same_cached_factory(
        self, mock_config, mock_provider
    ):
        header_factory = MagicMock()
        mock_provider.return_value = header_factory
        args = _resolve_sql(_SQL_OAUTH_CREDS)
        # The stored credentials_provider is a zero-arg callable that returns
        # the *cached* HeaderFactory. Calling it multiple times returns the
        # same object — this is what lets the SDK's TokenSource cache survive
        # across SQL connector invocations.
        cp = args["credentials_provider"]
        self.assertIs(header_factory, cp())
        self.assertIs(header_factory, cp())
        self.assertIs(cp(), cp())
