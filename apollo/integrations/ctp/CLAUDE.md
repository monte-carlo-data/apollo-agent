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
  to run the pipeline. If `creds` already contain `connect_args`, the pipeline is skipped
  (legacy / pre-transformed path).

## Adding a new connector

1. Create `apollo/integrations/ctp/defaults/<connector>.py` with a `TypedDict` for the
   output shape and a `CtpConfig` instance (follow `fabric.py` as the simplest pattern,
   or `starburst_galaxy.py` for a connector with transform steps).
2. Register it in `apollo/integrations/ctp/registry.py` inside `_discover()`:
   ```python
   from apollo.integrations.ctp.defaults.<connector> import MY_CONNECTOR_DEFAULT_CTP
   CtpRegistry.register("my-connector", MY_CONNECTOR_DEFAULT_CTP)
   ```
3. Update the proxy client (`__init__`) to accept `connect_args` as a dict and serialize
   it to the driver-specific format (see `MsFabricProxyClient` for the dict→ODBC pattern).

## Phase 2 migration plan

Several connectors (sql-server, azure-sql-database, azure-dedicated-sql-pool) have CTP
configs defined in `defaults/sql_server.py` but are **not yet registered** in `_discover()`.
Their proxy clients currently expect `connect_args` to be a pre-built string (sent by older
DC versions). Phase 2 will:
- Update those proxy clients to accept a dict (like `MsFabricProxyClient` does)
- Register their CTPs in `_discover()`

Do not register these until the corresponding proxy client is updated.

## Security note

Jinja2 templates are sandboxed (see `template.py`). Do not use `Environment()` directly —
always go through the pipeline so the sandbox is enforced.
