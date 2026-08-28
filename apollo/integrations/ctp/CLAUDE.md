# CTP — Credential Transform Pipeline

Transforms flat credential dicts (as sent by the Data Collector) into typed `connect_args`
before the proxy client is constructed. This decouples the DC's credential format from
the ODBC/driver-specific format each integration needs.

## Key concepts

- **`CtpConfig`** — a pipeline definition: a list of `TransformStep`s plus a final `MapperConfig`.
- **`MapperConfig`** — maps flat credential fields to output keys using Jinja2 templates
  (e.g. `"{{ raw.client_id }}"` → the value of `credentials["client_id"]`).
- **`TransformStep`** — an intermediate transformation step (e.g. decoding a PEM cert,
  constructing a derived field). Most simple integrations use `steps=[]`.
- **`CtpRegistry`** — the runtime registry. Call `CtpRegistry.resolve(connection_type, creds)`
  to run the pipeline. If `creds` already contain `connect_args` as a dict (DC pre-shaped path),
  the inner dict is unwrapped and run through the pipeline — both flat and pre-shaped credentials
  follow the same transform path. If `connect_args` is not a dict (e.g. a legacy pre-built ODBC
  string), the credentials are returned unchanged.

## Adding a new connector

1. Create `apollo/integrations/ctp/defaults/<connector>.py` with a `TypedDict` for the
   output shape and a `CtpConfig` instance (follow `sql_server.py` as a pattern for simple
   ODBC connectors, or `starburst_galaxy.py` for a connector with transform steps). For
   HTTP/OAuth connectors that need a custom resolve transform feeding the shared `oauth` step,
   follow `mulesoft.py`.
2. At module level in that file, call `CtpRegistry.register(...)`:
   ```python
   CtpRegistry.register("my-connector", MY_CONNECTOR_DEFAULT_CTP)
   ```
   Then add an import of that module inside `_discover()` in `apollo/integrations/ctp/registry.py`:
   ```python
   import apollo.integrations.ctp.defaults.<connector>  # noqa: F401
   ```
   If the connector introduces a new transform function (e.g. a custom resolve step), also
   register it inside `_discover()` in `apollo/integrations/ctp/transforms/registry.py`.
3. Update the proxy client (`__init__`) to accept `connect_args` as a dict and serialize
   it to the driver-specific format (see `MsFabricProxyClient` for the dict→ODBC pattern).
   If the connector reuses the generic `HttpProxyClient` (via `_get_proxy_client_http`), no
   proxy-client subclass is needed — the CTP just emits connect_args matching
   `HttpProxyClient`'s contract (`token`, `auth_type`, `ssl_verify`); the DC constructs full
   request URLs and calls `do_request` directly. Add
   `"my-connector": _get_proxy_client_http` to `_CLIENT_FACTORY_MAPPING` in
   `apollo/agent/proxy_client_factory.py`. See `mulesoft.py` and `defaults/mulesoft.py` as
   reference.

## CTP-enrolled connectors

**ODBC connectors** (sql-server, azure-sql-database, azure-dedicated-sql-pool, microsoft-fabric)
are fully migrated: their CTP configs in `defaults/sql_server.py` are registered in
`_discover()`. SQL Server / Azure variants retain a legacy pre-built ODBC string path for
backwards compatibility with older DC versions; Fabric requires a dict (CTP path only).

**HTTP/OAuth connectors** — MuleSoft (`mulesoft` connection type) is CTP-enrolled and uses
`HttpProxyClient` via `_get_proxy_client_http`. No ODBC string is involved; the pipeline
emits `token`, `auth_type`, and `ssl_verify` directly.

**GCP SDK connectors** — GCP Dataform (`gcp-dataform` connection type) is CTP-enrolled with a
dedicated `GcpDataformProxyClient`. The CTP config in `defaults/gcp_dataform.py` maps
`project_id`, `service_account_info`, and an optional `locations` list; the proxy client handles
SA credential construction and exposes Dataform API calls as serialized-dict methods.

**Oracle** (`oracle` connection type) — CTP config in `defaults/oracle.py` maps the scalar
connection fields (`dsn`/`user`/`password`/`expire_time`) and additionally passes the whole nested
`ssl_options` block through **unchanged**: `"ssl_options": "{{ raw.ssl_options | default(none) }}"`.
This is the reference for the **nested-dict passthrough pattern** — when a proxy client needs a
structured credential sub-object (not flat scalars), map it through as a single template
expression. The sandboxed `NativeEnvironment` (see `template.py`) preserves it as a `dict` rather
than stringifying it. `OracleProxyClient` then pops `ssl_options` out of `connect_args` (it is not
an `oracledb.connect` arg) and builds the thin `ssl.SSLContext` / thick wallet from it — the
resolution can't happen in the CTP because it depends on runtime/process state (thin vs thick).

**SQL Server Windows authentication** (`sql-server` with `auth_type: kerberos`) — the
reference for a transform whose artefacts are consumed as **process state**. The
`prepare_kerberos` step writes the krb5.conf, the keytab and the credential cache, then
passes their locations through as a nested `kerberos` dict (the same passthrough shape as
Oracle's `ssl_options` above). It deliberately sets **no environment variables**:
`KRB5_CONFIG` / `KRB5CCNAME` / `KRB5_CLIENT_KTNAME` are process-global and are also read by
Hive and Impala with `auth_mechanism=GSSAPI`, so setting them at CTP time left them
pointing at a single-realm config that gets deleted when the client closes.
`integrations/db/sql_server_kerberos_env.py` sets them around `pyodbc.connect` and restores
the previous values, and holds a lock for that whole scope because the variables are
shared. **If you write a transform whose output is environment, follow this split** — the
transform produces paths, the proxy client owns the mutation and its undo.

**Power BI** (`power-bi` connection type) — the reference for the **deferred / host-scoped auth
pattern**. Unlike every other enrolled connector, the CTP does _not_ resolve a token: its config
in `defaults/power_bi.py` has `steps=[]` and simply passes the raw MSAL credentials
(`auth_mode` + `client_id`/`tenant_id`/`client_secret` or `username`/`password`) — or a legacy
pre-shaped `token` — straight through to `connect_args`. Token acquisition happens _per request_
in `PowerBiProxyClient` (`apollo/integrations/powerbi/`), which overrides
`HttpProxyClient._attach_auth_header(headers, url)` and delegates to `PowerBiTokenProvider`
(`apollo/integrations/powerbi/msal_auth.py`). The provider selects the MSAL scope by destination
host — `api.powerbi.com` → the Power BI API scope, `api.fabric.microsoft.com` → the Microsoft
Fabric scope, any other host → no token — minting and caching a token per scope. This is the
precedent for connectors whose credential _audience_ is only known at request time, not CTP time.
(The generic `resolve_msal_token` transform that previously minted the Power BI token at CTP time
was removed with this change; `msal_auth.acquire_token` is now the single home for the MSAL logic.)

## Security note

Jinja2 templates are sandboxed (see `template.py`). Do not use `Environment()` directly —
always go through the pipeline so the sandbox is enforced.

## Temp-file cleanup

Transforms that materialize a file on disk (cert/key/ini — e.g. `tmp_file_write`,
`write_ini_file`, `resolve_ssl_options`) **must** append the path to `state.temp_files`.
The pipeline surfaces this list via the optional `temp_files` out-param on
`CtpRegistry.resolve()` / `resolve_custom()` (and `CtpPipeline.execute()`); the proxy client
factory passes it to the constructed client via `BaseProxyClient.register_temp_files()`, which
deletes the files when the client is closed. If you add a transform that writes a file and forget
to append its path, the file persists for the container lifetime — the credential-leak class of
bug this mechanism exists to prevent. Prefer a unique path per file (`mkstemp`) over a
deterministic one so one client's cleanup can't delete a file still in use by another.

When calling `CtpRegistry.resolve()` directly (outside the factory — e.g. in a test), pass a
`temp_files=[]` list and clean those paths up yourself, or the materialized files will leak.
