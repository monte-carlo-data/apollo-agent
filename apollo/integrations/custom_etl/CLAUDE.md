# Custom ETL Connectors

Runtime-loaded ETL connectors baked into the Docker image at `/opt/custom-etl-connectors`.
Each connector provides a `manifest.json` and a `connector.py` (implementing a `Connector`
class that inherits from `BaseEtlConnector`).

## Key modules

- **`custom_etl_connector_loader.py`** — discovers connectors on the filesystem, loads modules
  dynamically via `importlib.util`, and caches the registry at module level.
- **`custom_etl_proxy_client.py`** — `CustomEtlProxyClient(BaseProxyClient)` wraps a loaded
  connector module. Exposes `fetch_etl_assets` and `fetch_etl_runs` which delegate to the
  connector's `fetch_metadata` and `_fetch_run_details` methods, serializing the model objects
  (EtlAsset/EtlRunEvent) to dicts for the data-collector.

## Opt-in gating

Custom ETL connectors share the same gate as custom connectors: the env var
`MCD_CUSTOM_CONNECTORS_ENABLED=true`. The factory in `apollo/agent/proxy_client_factory.py`
checks this before falling through to either custom connector path.

## Connector directory structure

```
/opt/custom-etl-connectors/<name>/
├── manifest.json        # connection_type, name, terminology, icon_url
└── connector.py         # Connector(BaseEtlConnector) class
```

## Manifest format

```json
{
  "connection_type": "custom-etl-connector-<hash>",
  "name": "adf",
  "terminology": {
    "group": "Factory",
    "job": "Pipeline",
    "task": "Activity"
  },
  "icon_url": "https://example.com/icon.png"
}
```

## Connector interface

The `connector.py` module must define a `Connector` class with:
- `credentials` attribute (set by the proxy before `setup_connection`)
- `setup_connection()` — establish connection using credentials
- `close_connection()` — clean up resources
- `fetch_metadata(limit, offset)` → `List[EtlAsset]`
- `_fetch_run_details(run_ids, lookback, limit, offset)` → `List[EtlRunEvent]`

## How it differs from custom (warehouse) connectors

| Aspect | Custom connectors | Custom ETL connectors |
|--------|------------------|-----------------------|
| Base path | `/opt/custom-connectors` | `/opt/custom-etl-connectors` |
| Connector class | `BaseConnector` | `Connector(BaseEtlConnector)` |
| Templates | Jinja2 `.j2` SQL templates | None (code-only) |
| Methods | SQL-oriented (fetch_tables, etc.) | ETL-oriented (fetch_metadata, runs) |
| Type prefix | `custom-connector-<hash>` | `custom-etl-connector-<hash>` |
