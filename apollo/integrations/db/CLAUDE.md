# DB Proxy Clients

Each file in this directory is a proxy client for a database integration. All clients
inherit from `BaseDbProxyClient` (which inherits `BaseProxyClient`).

## Key conventions

### `connect_args` credential key

The standard credential key for connection details is `connect_args`. The value may be:
- A **string** — a pre-built driver-specific connection string (legacy path, sent by older DCs).
- A **dict** — a structured map of connection parameters produced by the CTP pipeline
  (preferred path for new integrations).

Proxy clients that accept a dict must serialize it to the driver format in `__init__`.
See `MsFabricProxyClient._odbc_escape` for the ODBC dict→string serialization pattern
(values with special chars must be brace-escaped per the ODBC spec).

### pyodbc clients

Several clients use `pyodbc` (fabric, azure_database, sql_server). They share:
- `_DATETIMEOFFSET_SQL_TYPE_CODE = -155` — output converter for SQL Server's datetimeoffset type
- `_handle_datetimeoffset(dto_value)` — converts the raw bytes to a timezone-aware `datetime`
- `_process_description(col)` — overrides base class to use `col[1].__name__` (pyodbc returns
  the Python type object, not a type code)
- Default timeouts: `login_timeout=15s`, `query_timeout_in_seconds=840s` (14 minutes)

These are currently duplicated across the three pyodbc clients; a shared base class is a
planned follow-up.

### Connection lifecycle

Connections are opened in `__init__` and closed in `__del__` (via `BaseDbProxyClient.close`).
The `wrapped_client` property exposes the underlying connection/client for the agent framework.

### Non-pyodbc clients

Most other clients (postgres, mysql, bigquery, etc.) wrap their own driver's connection object.
Follow the existing pattern for each driver — there is no single universal pattern beyond the
base class interface.

## Adding a new DB integration

1. Create `<name>_proxy_client.py` following the closest existing pattern.
2. Register a factory function in `apollo/agent/proxy_client_factory.py`.
3. If the integration uses flat credentials, add a CTP config in
   `apollo/integrations/ctp/defaults/` and register it — see `apollo/integrations/ctp/CLAUDE.md`.
4. Add tests in `tests/test_<name>_client.py` following `test_ms_fabric_client.py` or
   `test_azure_dedicated_sql_pool_client.py`.
