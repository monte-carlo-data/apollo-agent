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
- **`connector_patches.py`** — `install_connector_patches()`: a retired no-op. Don't
  delete it while data-collector still imports it, or they crash with an `ImportError`.

## Duplicate column names

`databricks-sql-connector` 4.2.6–4.7.x rejected Arrow results with duplicate column names
(`ArrowInvalid: Can't unify schema with duplicate field names`), e.g. `SELECT a.*, b.*`
over tables sharing a column. 4.8.0 fixed it upstream (databricks-sql-python#989), so keep
the connector at 4.8.0 or later. `tests/test_databricks_duplicate_column_names.py` runs the
connector's own concat and `fetchall` on duplicate names, so a connector bump that
regresses fails there.
