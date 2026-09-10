# Custom BI Connectors

Runtime-loaded BI connectors baked into the Docker image at `/opt/custom-bi-connectors`.
Each connector provides a `manifest.json` and a `connector.py` (implementing a `Connector`
class exposing only `fetch_metadata`).

## Key modules

- **`custom_bi_connector_loader.py`** — discovers connectors on the filesystem, loads modules
  dynamically via `importlib.util`, and caches the registry at module level.
- **`custom_bi_proxy_client.py`** — `CustomBiProxyClient(BaseProxyClient)` wraps a loaded
  connector module. Exposes `fetch_bi_metadata` which delegates to the connector's
  `fetch_metadata` method, serializing the model objects (BiAsset) to dicts for the
  data-collector.

## Wire serialization (`_serialize`)

The proxy is **decoupled from the model definitions** — those ship inside the connector
bundle (pycarlo `features.ingestion.bi`), not this repo. `_serialize` is therefore generic
and **field-name-agnostic**: it recurses through dataclasses (`dataclasses.asdict`), objects
with `__dict__`, dicts, and lists, preserving whatever nested structure the connector returns
and stripping `None` values. New fields a connector adds are carried through automatically.
`Enum` members serialize to their `.value` and `datetime`/`date` to ISO-8601 strings. Because
the wire carries SOURCE ids, the backend mints global ids.

## Opt-in gating

Custom BI connectors share the same gate as custom connectors: the env var
`MCD_CUSTOM_CONNECTORS_ENABLED=true`. The factory in `apollo/agent/proxy_client_factory.py`
checks this before falling through to any custom connector path.

## Connector directory structure

```
/opt/custom-bi-connectors/<name>/
├── manifest.json        # connection_type, connection_name, icon_url, credentials_schema (optional)
└── connector.py         # Connector class
```

## Manifest format

```json
{
  "connection_type": "custom-bi-connector-<hash>",
  "connection_name": "tableau",
  "icon_url": "https://example.com/icon.png"
}
```

The optional `credentials_schema` key accepts a cerberus schema dict for self-hosted credential validation.

The agent reads `connection_type` (registry routing) and `connection_name` (display), and passes the
rest of the manifest through to the backend untouched (stripping only `credentials_schema`) — the
backend is what reads keys like `asset_class` to route the manifest class.

## Connector interface

The `connector.py` module must define a `Connector` class with:
- `credentials` attribute (set by the proxy before `setup_connection`)
- `setup_connection()` — establish connection using credentials
- `close_connection()` — clean up resources
- `fetch_metadata(limit, offset)` → `List[BiAsset]`

## How it differs from custom ETL connectors

| Aspect | Custom ETL connectors | Custom BI connectors |
|--------|----------------------|----------------------|
| Base path | `/opt/custom-etl-connectors` | `/opt/custom-bi-connectors` |
| Methods | `fetch_metadata` + `fetch_run_details` | `fetch_metadata` only |
| Run pipeline | Runs + webhook, `run_status_mapping` | None (BI assets have no runs) |
| Type prefix | `custom-etl-connector-<hash>` | `custom-bi-connector-<hash>` |
