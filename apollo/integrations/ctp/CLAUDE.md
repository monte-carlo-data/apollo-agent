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
   output shape and a `CtpConfig` instance (follow `fabric.py` as the simplest pattern,
   or `starburst_galaxy.py` for a connector with transform steps).
2. At module level in that file, call `CtpRegistry.register(...)`:
   ```python
   CtpRegistry.register("my-connector", MY_CONNECTOR_DEFAULT_CTP)
   ```
   Then add an import of that module inside `_discover()` in `apollo/integrations/ctp/registry.py`:
   ```python
   import apollo.integrations.ctp.defaults.<connector>  # noqa: F401
   ```
3. Update the proxy client (`__init__`) to accept `connect_args` as a dict and serialize
   it to the driver-specific format (see `MsFabricProxyClient` for the dict→ODBC pattern).

## Phase 2 migration status

Phase 2 connectors (sql-server, azure-sql-database, azure-dedicated-sql-pool) are now fully
migrated: their CTP configs in `defaults/sql_server.py` are registered in `_discover()`, and
their proxy clients accept `connect_args` as either a dict (CTP path) or a pre-built string
(legacy DC path). The string path is retained for backwards compatibility with older DC versions
that pre-build the ODBC string before sending.

## Security note

Jinja2 templates are sandboxed (see `template.py`). Do not use `Environment()` directly —
always go through the pipeline so the sandbox is enforced.
