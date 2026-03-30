---
name: add-integration
description: Use when adding a new database integration to apollo-agent — creates the proxy client, CTP default config, factory registration, and tests
argument-hint: "<connection-type-name>"
---

# Add a Database Integration

## Overview

Every new database integration requires exactly **5 file changes**:

| Step | File | What changes |
|------|------|-------------|
| 1 | `apollo/integrations/db/<name>_proxy_client.py` | New proxy client class |
| 2 | `apollo/integrations/ctp/defaults/<name>.py` | CTP config: flat creds → typed `connect_args` dict |
| 3 | `apollo/agent/proxy_client_factory.py` | Register the new connection type |
| 4 | `apollo/integrations/ctp/registry.py` | Wire CTP into `_discover()` |
| 5 | `tests/test_<name>_client.py` | Tests |

> **Relationship to `/add-ccp-connector`**: That skill covers Steps 2 and 4 (CTP config + registry) in depth — use it for those steps. This skill orchestrates the full integration and delegates CTP details there.

---

## Step 1: Proxy Client

**File:** `apollo/integrations/db/<name>_proxy_client.py`

All proxy clients subclass `BaseDbProxyClient` and accept `connect_args` as a **dict** (produced by the CTP pipeline). The minimum structure:

```python
from typing import Optional, Any

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class <Name>ProxyClient(BaseDbProxyClient):
    """Proxy client for <Name> connections."""

    def __init__(self, credentials: Optional[dict], **kwargs: Any):
        super().__init__(connection_type="<connection-type-name>")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"<Name> agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        connect_args = credentials[_ATTR_CONNECT_ARGS]
        if not isinstance(connect_args, dict):
            raise ValueError(
                f"{_ATTR_CONNECT_ARGS} must be a dict, got {type(connect_args).__name__}"
            )
        self._connection = <driver>.connect(**connect_args)

    @property
    def wrapped_client(self):
        return self._connection
```

### ODBC / pyodbc clients (SQL Server family)

For pyodbc-based integrations, inherit from `TSqlBaseDbProxyClient` instead of `BaseDbProxyClient` — it provides `_process_description` and `_handle_datetimeoffset` shared by all T-SQL clients. Serialize the `connect_args` dict to an ODBC connection string:

```python
from typing import Optional, Any

import pyodbc

from apollo.integrations.db.tsql_base_db_proxy_client import TSqlBaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


def _odbc_escape(value: str) -> str:
    if value.startswith("{") and value.endswith("}"):
        return value  # already wrapped (e.g. driver names)
    if any(c in value for c in (";", "{", "}", "=")):
        return "{" + value.replace("}", "}}") + "}"
    return value


class <Name>ProxyClient(TSqlBaseDbProxyClient):

    _DEFAULT_LOGIN_TIMEOUT_IN_SECONDS = 15
    _DEFAULT_QUERY_TIMEOUT_IN_SECONDS = 60 * 14  # 14 minutes

    def __init__(self, credentials: Optional[dict], **kwargs: Any):
        super().__init__(connection_type="<connection-type-name>")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(...)
        connect_args = credentials[_ATTR_CONNECT_ARGS]
        if not isinstance(connect_args, dict):
            raise ValueError(...)
        connection_string = ";".join(
            f"{k}={_odbc_escape(str(v))}" for k, v in connect_args.items()
        )
        self._connection = pyodbc.connect(
            connection_string,
            timeout=credentials.get("login_timeout", self._DEFAULT_LOGIN_TIMEOUT_IN_SECONDS),
        )
        self._connection.add_output_converter(
            self._DATETIMEOFFSET_SQL_TYPE_CODE, self._handle_datetimeoffset
        )
        self._connection.timeout = credentials.get(
            "query_timeout_in_seconds", self._DEFAULT_QUERY_TIMEOUT_IN_SECONDS
        )
```

**Always include `_odbc_escape`** — ODBC values containing `;`, `{`, `}`, or `=` must be brace-wrapped per the ODBC spec.

### Non-ODBC clients (trino, psycopg2, etc.)

See `starburst_proxy_client.py` (trino) or `postgres_proxy_client.py` (psycopg2) for patterns.
For trino, `connect_args` is passed directly as kwargs: `trino.dbapi.connect(**connect_args)`.

---

## Step 2: CTP Default Config

**File:** `apollo/integrations/ctp/defaults/<name>.py`

**→ Run `/add-ccp-connector` for detailed guidance** on the TypedDict schema, `CtpConfig`/`MapperConfig` structure, Jinja2 template rules, optional fields, transform steps, and SSL cert materialization.

Reference implementations: `apollo/integrations/ctp/defaults/fabric.py` (ODBC), `apollo/integrations/ctp/defaults/starburst_galaxy.py` (trino), `apollo/integrations/ctp/defaults/postgres.py` (psycopg2).

### Azure AD service principal auth (ODBC Driver 18)

If the integration uses Azure AD service principal auth, the tenant ID is encoded in the ODBC `UID` field — this format is Azure-specific:

```python
"Authentication": "ActiveDirectoryServicePrincipal",
"UID": "{{ raw.client_id }}@{{ raw.tenant_id }}",
"PWD": "{{ raw.client_secret }}",
```

Three required flat credential fields: `client_id`, `client_secret`, `tenant_id`.

---

## Step 3: Factory Registration

**File:** `apollo/agent/proxy_client_factory.py`

Add a factory function and a mapping entry. Follow the existing lazy-import pattern:

```python
def _get_proxy_client_<name>(
    credentials: Optional[dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.db.<name>_proxy_client import <Name>ProxyClient

    return <Name>ProxyClient(credentials=credentials, platform=platform)
```

Add to `_CLIENT_FACTORY_MAPPING`:

```python
"<connection-type-name>": _get_proxy_client_<name>,
```

Place alphabetically in the dict.

---

## Step 4: CTP Registry

**File:** `apollo/integrations/ctp/registry.py`

**→ `/add-ccp-connector` covers this step.** In short: add two lines to `_discover()`:

```python
from apollo.integrations.ctp.defaults.<name> import <NAME>_DEFAULT_CTP
CtpRegistry.register("<connection-type-name>", <NAME>_DEFAULT_CTP)
```

> **Note:** Only register here once the proxy client (Step 1) accepts `connect_args` as a dict. Registering before that will silently skip CTP for flat credentials.

---

## Step 5: Tests

**File:** `tests/test_<name>_client.py`

**Canonical reference:** `tests/test_ms_fabric_client.py`

Minimum test coverage:

```python
# Happy path — dict connect_args → correct driver call
@patch("<driver>.connect")
def test_connect_args_dict(self, mock_connect): ...

# Error — missing connect_args
def test_missing_connect_args_raises(self): ...

# Error — wrong connect_args type
def test_invalid_connect_args_type_raises(self): ...

# CTP round-trip — flat creds → resolve → correct connect_args → driver call
def test_ctp_registered(self): ...
def test_ctp_resolves_flat_credentials(self): ...
@patch("<driver>.connect")
def test_ctp_to_proxy_client_end_to_end(self, mock_connect): ...

# CTP bypass — connect_args already present → unchanged
def test_ctp_bypasses_when_connect_args_present(self): ...
```

For ODBC integrations, also test value escaping and datetimeoffset:

```python
@patch("pyodbc.connect")
def test_dict_value_with_semicolon_is_escaped(self, mock_connect):
    tricky = "p@ss;word=1"
    creds = {**_CONNECT_ARGS_DICT, "PWD": tricky}
    <Name>ProxyClient(credentials={"connect_args": creds}, platform="test")
    call_args = mock_connect.call_args[0][0]
    self.assertIn("PWD={p@ss;word=1}", call_args)
    self.assertNotIn("PWD=p@ss;word=1", call_args)

def test_handle_datetimeoffset(self): ...
```

---

## Reference Implementations

| Driver | Proxy client | CTP config |
|--------|-------------|------------|
| pyodbc (SQL Server / Fabric) | `fabric_proxy_client.py` (inherits `TSqlBaseDbProxyClient`) | `ctp/defaults/fabric.py` |
| trino (Starburst) | `starburst_proxy_client.py` | `ctp/defaults/starburst_galaxy.py` |
| psycopg2 (Postgres) | `postgres_proxy_client.py` | `ctp/defaults/postgres.py` |

---

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Bracket notation in template: `raw['port']` | Use `raw.port` |
| Missing `_odbc_escape` for ODBC dict serialization | Values with `;`, `{`, `}`, `=` must be brace-wrapped — copy `_odbc_escape` from `fabric_proxy_client.py` |
| Azure AD: `UID` missing `@tenant_id` | Use `{{ raw.client_id }}@{{ raw.tenant_id }}` |
| Registering CTP before proxy client supports dict | Wait until Step 1 accepts dict `connect_args` |
| Forgetting `_discover()` import | Without it, `CtpRegistry.get("<name>")` returns `None` and flat creds will fail |
| Adding `| int` / `| float` type filters | Remove — NativeEnvironment preserves Python types automatically |
| Using `connect()` return_value instead of direct attrs in mock cursor | Set `cursor.description` and `cursor.rowcount` as attributes, not `return_value` |
