# GCP Dataform Proxy Client

Thin proxy around the `google-cloud-dataform` SDK (`google-cloud-dataform` PyPI package /
`dataform_v1` import). No business logic, filtering, or enum mapping — all interpretation
lives in the data-collector client.

## Key conventions

### Dual usage mode

`GcpDataformProxyClient` is used in two modes:

- **Agent-side**: instantiated by the proxy client factory when the DC delegates via
  `@agent_operation`.
- **DC-side (direct)**: instantiated directly by the DC client when no remote agent is
  configured, since DC depends on apollo-agent.

### proto-plus `to_dict()` serialization

Every method returns plain `dict` / `list[dict]` via the proto-plus class method
`type(obj).to_dict(obj)`. This is intentional — proto-plus message objects are not
JSON-serializable; calling `to_dict()` via the concrete type (not an instance method)
handles enum coercion and nested message flattening correctly.

### `connect_args` credential shape

Required keys inside `credentials["connect_args"]`:

| Key | Description |
|-----|-------------|
| `project_id` | GCP project ID |
| `service_account_info` | Service account JSON key as a `dict` |

Optional:

| Key | Description |
|-----|-------------|
| `locations` | List of GCP region strings (e.g. `["us-central1"]`); used by `test_connection` and `get_connection_metadata` |

### Adding new methods

1. Look up the SDK call in the [Dataform API reference](https://cloud.google.com/python/docs/reference/dataform/latest).
2. Follow the existing pattern: call the SDK, serialize each result with `cast(dict, type(r).to_dict(r))`.
3. For paginated list calls, iterate the pager directly (the SDK handles pagination).
4. For calls requiring a request object, build it explicitly (e.g. `dataform_v1.QueryCompilationResultActionsRequest`) — see `query_compilation_result_actions` for a working example.
5. Register the new proxy client method in the DC client's `@agent_operation`-decorated wrapper.
