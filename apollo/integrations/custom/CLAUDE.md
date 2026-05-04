# Custom Connectors

Runtime-loaded database connectors baked into the Docker image at `/opt/custom-connectors`.
Each connector provides a `manifest.json`, a `connector.py` (implementing `BaseConnector`),
and optional Jinja2 query templates.

## Key modules

- **`custom_connector_loader.py`** — discovers connectors on the filesystem, loads modules
  dynamically via `importlib.util`, and caches the registry at module level.
- **`custom_proxy_client.py`** — `CustomProxyClient(BaseProxyClient)` wraps a loaded connector
  module. Compiles 5 core Jinja2 templates at init; other templates available as raw strings.

## Opt-in gating

Custom connectors are only used when the env var `MCD_CUSTOM_CONNECTORS_ENABLED=true` is set.
The factory in `apollo/agent/proxy_client_factory.py` checks this before falling through to
the custom connector path.

## Connector directory structure

```
/opt/custom-connectors/<name>/
├── manifest.json        # connection_type, connection_name, capabilities, metrics
├── connector.py         # BaseConnector class (create_connection, execute_query, etc.)
└── templates/           # optional Jinja2 .j2 query templates
```

## Adding a new custom connector

Custom connectors are not added to the codebase — they are user-provided and discovered at
runtime. See the connector directory structure above for what each connector must provide.

## Security

Jinja2 templates are rendered inside an `ImmutableSandboxedEnvironment` to prevent code
injection. Module loading uses `importlib.util.spec_from_file_location` with a unique module
name per connector to avoid namespace collisions.
