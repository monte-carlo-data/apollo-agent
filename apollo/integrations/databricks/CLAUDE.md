# Databricks — SQL Warehouse and REST Proxy Clients

## File responsibilities

- **`databricks_sql_warehouse_proxy_client.py`** — `DatabricksSqlWarehouseProxyClient`:
  `BaseDbProxyClient` over `databricks-sql-connector` (`databricks.sql`). Credentials arrive
  as `connect_args` from the CTP pipeline and go straight to `sql.connect`; the client
  rejects `connect_args` without a PAT (`access_token`) or OAuth `credentials_provider`,
  since the connector would otherwise open a browser login and hang.
- **`databricks_rest_proxy_client.py`** — `DatabricksRestProxyClient`: Databricks REST API
  calls over `HttpProxyClient`, using the token the CTP `resolve_databricks_token` transform
  resolves. Doesn't use the connector.
- **`connector_patches.py`** — `install_connector_patches()`: runtime patches for
  `databricks-sql-connector` behaviour we can't fix by pinning.

## Connector patches

`databricks-sql-connector` 4.2.6+ rejects any Arrow result with duplicate column names
(`ArrowInvalid: Can't unify schema with duplicate field names`), e.g. `SELECT a.*, b.*`
over tables sharing a column. `connector_patches.py` wraps the connector's private
`databricks.sql.utils._concat_arrow_tables` to fall back to a plain `concat_tables` only
when every chunk has the same schema and that schema contains a duplicate name; any other
error is re-raised unchanged. Downgrading to 4.2.5 isn't an option: it caps thrift below
the CVE-fixed 0.24.

- **Every module that imports `databricks.sql` must call `install_connector_patches()` at
  module level.** Today that's only `databricks_sql_warehouse_proxy_client.py`.
  `test_every_databricks_sql_importer_installs_patch` scans `apollo/` and fails if a new
  importer skips the call.
- **Re-check the patch on every connector bump.** `DatabricksConnectorVersionTests` in
  `tests/test_databricks_connector_patches.py` pins the connector to 4.5.x–4.6.x, so a bump
  outside that range fails. Then `test_unpatched_connector_still_rejects_duplicate_names`
  tells you what to do: if it passes, the bug is still there, so re-validate the patch and
  widen the range; if it fails, upstream fixed it, so delete `connector_patches.py`.
