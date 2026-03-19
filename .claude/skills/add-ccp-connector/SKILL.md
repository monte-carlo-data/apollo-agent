---
name: add-ccp-connector
description: Use when adding a new connector to the apollo-agent CCP (Client Creation Pipeline) — registering a new connection type so flat credentials are transformed into typed connect_args before the proxy client is created
---

# Add a CCP Connector

## Overview

Each CCP connector is a declarative config that transforms flat credentials (e.g. `{host, port, database, user, password}`) into a typed `connect_args` dict before the proxy client factory creates the client. Postgres is the reference implementation.

## Every new connector requires exactly two file changes

### 1. Create `apollo/integrations/ccp/defaults/<connector>.py`

```python
from typing import TypedDict, Required, NotRequired

from apollo.integrations.ccp.models import CcpConfig, MapperConfig, TransformStep


class ExampleClientArgs(TypedDict):
    host: Required[str]
    port: Required[int]
    dbname: Required[str]
    user: Required[str]
    password: Required[str]
    sslmode: NotRequired[str]       # use NotRequired for optional fields


EXAMPLE_DEFAULT_CCP = CcpConfig(
    name="example-default",
    steps=[],                        # add TransformSteps here if needed (see below)
    mapper=MapperConfig(
        name="example_client_args",
        schema=ExampleClientArgs,
        field_map={
            "host":     "{{ raw.host }}",
            "port":     "{{ raw.port }}",
            "dbname":   "{{ raw.database }}",
            "user":     "{{ raw.user }}",
            "password": "{{ raw.password }}",
            # Optional fields: use default(none) — None values are automatically omitted
            "sslmode":  "{{ raw.ssl_mode | default(none) }}",
        },
    ),
)

from apollo.integrations.ccp.registry import CcpRegistry  # noqa: E402

CcpRegistry.register("example", EXAMPLE_DEFAULT_CCP)
```

### 2. Add the import to `_discover()` in `apollo/integrations/ccp/registry.py`

```python
def _discover() -> None:
    import apollo.integrations.ccp.defaults.postgres  # noqa: F401
    import apollo.integrations.ccp.defaults.example   # noqa: F401  ← add this
```

That's it. `_create_proxy_client` in `proxy_client_factory.py` checks `CcpRegistry.get(connection_type)` and calls `resolve()` automatically — no changes needed there.

---

## Jinja2 template rules

**Always use dot-notation (`raw.field`), never bracket notation (`raw['field']`)**

Dot-notation returns `Undefined` for missing keys so `default()` and `is defined` work correctly. Bracket notation raises immediately.

```
{{ raw.port }}                        # returns native Python type as-is (int stays int)
{{ raw.ssl_mode | default('require') }} # substitute when missing
{{ raw.ssl_mode | default(none) }}    # omit the field when missing (None is filtered out)
```

**NativeEnvironment preserves Python types** — you do not need `| int`, `| float`, or `| bool` filters unless the upstream value is actually a string and you need to coerce it. Never add type filters speculatively.

---

## Adding a TransformStep (e.g. SSL cert → temp file)

```python
steps=[
    TransformStep(
        type="tmp_file_write",
        when="raw.ssl_ca_pem is defined",   # condition uses dot-notation
        input={
            "contents":    "{{ raw.ssl_ca_pem }}",
            "file_suffix": ".pem",
            "mode":        "0400",
        },
        output={"path": "ssl_ca_path"},     # written to state.derived
        field_map={
            # contributed to client_args only when this step runs
            "sslrootcert": "{{ derived.ssl_ca_path }}",
            "sslmode":     "{{ raw.ssl_mode | default('require') }}",
        },
    )
],
```

`step.field_map` overrides `mapper.field_map` on collision. Read the Postgres default for a complete working example.

---

## TypedDict schema

The schema validates **key names only** (required presence, no unknown keys) — it does **not** validate types. Use it to document the contract and catch field name typos.

---

## Legacy credentials passthrough

`CcpRegistry.resolve()` returns credentials unchanged if they already contain a `connect_args` key. DC plugins currently pre-map credentials to `connect_args` before sending, so the CCP is bypassed for those callers today. No special handling needed in the connector definition.

---

## Tests to write

Add to `tests/ccp/test_registry.py`:

```python
def test_example_registered(self):
    config = CcpRegistry.get("example")
    self.assertIsNotNone(config)
    self.assertEqual("example-default", config.name)

def test_resolve_flat_example_credentials(self):
    result = CcpRegistry.resolve("example", {
        "host": "db.example.com", "port": 5432,
        "database": "mydb", "user": "admin", "password": "secret",
    })
    self.assertIn("connect_args", result)
    self.assertEqual("db.example.com", result["connect_args"]["host"])
    self.assertEqual("mydb", result["connect_args"]["dbname"])

def test_resolve_legacy_example_credentials_unchanged(self):
    legacy = {"connect_args": {"host": "h", "dbname": "d"}}
    self.assertEqual(legacy, CcpRegistry.resolve("example", legacy))
```

If the connector has an SSL transform step, also add a test that provides the SSL input field and asserts the output path exists (see `test_resolve_flat_postgres_with_ssl_ca_pem` in the existing registry tests for the pattern).

---

## Common mistakes

| Mistake | Fix |
|---|---|
| Bracket notation in template: `raw['port']` | Use `raw.port` |
| Adding `\| int` / `\| float` filters | Remove — NativeEnvironment preserves Python types |
| Emitting `sslmode` unconditionally | Use `default(none)` or put in step `field_map` so it only appears when needed |
| Forgetting `_discover()` import | Without it, `CcpRegistry.get("example")` returns `None` and CCP is silently skipped |
| SSL logic duplicated from proxy client | Leave SSL handling that already exists in the proxy client; only use CCP transforms for cert materialization (writing PEM strings to temp files) |
