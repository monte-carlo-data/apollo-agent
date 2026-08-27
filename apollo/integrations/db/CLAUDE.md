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
See `odbc_string_from_dict` in `tsql_base_db_proxy_client.py` for the ODBC dict→string serialization pattern
(values with special chars must be brace-escaped per the ODBC spec).

### pyodbc clients

Several clients use `pyodbc` (fabric, azure_database, sql_server). They share:
- `_DATETIMEOFFSET_SQL_TYPE_CODE = -155` — output converter for SQL Server's datetimeoffset type
- `_handle_datetimeoffset(dto_value)` — converts the raw bytes to a timezone-aware `datetime`
- `_process_description(col)` — overrides base class to use `col[1].__name__` (pyodbc returns
  the Python type object, not a type code)
- Default timeouts: `login_timeout=15s`, `query_timeout_in_seconds=840s` (14 minutes). These keys are passed inside `connect_args` and popped before the dict is serialized to an ODBC string.

These are shared via `TSqlBaseDbProxyClient` in `tsql_base_db_proxy_client.py`, which all three clients inherit from.

### Connection lifecycle

Connections are opened in `__init__` and closed in `__del__` (via `BaseProxyClient.close`).
The `wrapped_client` property exposes the underlying connection/client for the agent framework.

To release the connection, override **`_close_client()`** — not `close()`. `BaseProxyClient.close()`
is a template method that runs `_close_client()` and then deletes any temp credential files
registered via `register_temp_files` (in a `finally`, so cleanup runs even if the connection
teardown raises). Overriding `close()` directly silently skips that cleanup.

### Cached credentials

A client may cache a minted secret (e.g. an OAuth token) to avoid re-minting on every
call. Scope the cache deliberately: an **instance attribute does not survive a single
operation**, because the data-collector sends `skip_cache=true` on every agent op, so
`ProxyClientFactory` builds a fresh proxy client per call and closes it immediately —
`_close_client` never runs long enough to matter. To dedupe across calls the cache must be
**process-wide** (module-level), keyed by a hash of the identifying credential fields so a
token is only ever served back to byte-identical credentials. Bound its lifetime explicitly
(a soft max-age) rather than relying on `close()`, guard it with a lock (the agent runs
multi-threaded — see the Dockerfile `--threads` setting), and detect secret
expiry/revocation reactively on the auth-failure response rather than with a hopeful TTL. See
`salesforce_data_cloud_proxy_client._get_or_mint_core_token` and its `_CORE_TOKEN_CACHE` for
the reference implementation (YET-2522).

### Credential temp files

If `__init__` (or a helper like `get_cert_path`) materializes a cert/key/CA file on disk, register
its path with `self.register_temp_files([path])` so it is deleted when the client closes — otherwise
the file persists for the container lifetime. `get_cert_path` already registers what it downloads;
clients that write their own CA file (db2, teradata) register it explicitly. Prefer a unique path
per client (e.g. `SslOptions.write_ca_data_to_temp_file`, which uses `mkstemp`) rather than a
deterministic one, so closing one client cannot delete a file still in use by another.

**Exception — Oracle thick mode.** `oracle_client_config.py`'s thick-mode TLS wallet and
`sqlnet.ora` config dir are **process-global** (built once, cached in module state, reused by every
connection) and are intentionally **not** registered via `register_temp_files`. Oracle Instant
Client freezes its trust store at the first `init_oracle_client`, so the wallet must outlive any
single client — per-client cleanup would delete a wallet still needed by later connections. See the
`oracle_client_config` module docstring for the full rationale.

### Concurrent batch reads (agent-side fan-out)

An op that would otherwise make the data-collector issue N sequential agent round-trips can instead
expose a **single batched method that fans out concurrently inside the agent process**. The agent
executes an op's recorded commands *sequentially* (`AgentClient._calls` is a shared list and is not
thread-safe), so the DC cannot parallelize round-trips by recording several `ssot_get` commands —
the parallelism has to live in one agent method. See
`salesforce_data_cloud_proxy_client.ssot_get_offset_pages`, which pulls N consecutive offset pages
with an internal `ThreadPoolExecutor` (YET-2531 follow-up).

Rules for that pattern:

- **The agent owns its bounds.** Clamp width (`_SSOT_OFFSET_PAGES_MAX`) and per-page limit inside the
  method — never trust the caller's numbers. Validate inputs (reject a base path that already carries
  the paginated params) and fail the whole batch *before* any request when they're malformed.
- **One page's failure is not the batch's failure.** Return a map keyed by page index where each entry
  is either `{"result": <body>}` or a structured `{"error", "error_type", "status_code",
  "error_code"}`. The caller decides what a hole means; the op never turns a single 404/quota into a
  batch-wide exception. Keep this envelope shape in lockstep with the consumer's translation tests.
- **Retry transient, never quota.** Give each page one retry within its timeout envelope for
  network/5xx blips, but let quota errors (`_is_ssot_quota_error`) fall straight through — retrying an
  exhausted org just deepens the hole.
- **Per-worker sessions, one shared token.** Each worker builds its own `_CapturingSession` (a shared
  `requests.Session` cross-contaminates under threads), but they all draw from the process-wide core
  token cache, so a cold-cache batch mints exactly once (see **Cached credentials**).

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
