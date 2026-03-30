---
name: add-integration
description: Use when adding a new database integration to apollo-agent — creates the proxy client, CTP default config, factory registration, and tests following the MS Fabric pattern
argument-hint: "<connection-type-name>"
---

# Add a Database Integration

## Overview

Every new database integration requires exactly **4 file changes**:

| Step | File | What changes |
|------|------|-------------|
| 1 | `apollo/integrations/db/<name>_proxy_client.py` | New proxy client class |
| 2 | `apollo/integrations/ctp/defaults/<name>.py` | CTP config: flat creds → typed `connect_args` dict |
| 3 | `apollo/agent/proxy_client_factory.py` | Register the new connection type |
| 4 | `apollo/integrations/ctp/registry.py` | Wire CTP into `_discover()` |
| 5 | `tests/test_<name>_client.py` | Tests |

> **Relationship to `/add-ccp-connector`**: That skill covers Step 2 (CTP config) in depth — use it for the CTP authoring step. This skill orchestrates the full integration and delegates CTP details there.

---

## Step 1: Proxy Client

**File:** `apollo/integrations/db/<name>_proxy_client.py`

**Canonical reference:** `apollo/integrations/db/fabric_proxy_client.py` (pyodbc/ODBC pattern)

```python
import struct
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Union

import <driver>  # e.g. pyodbc, trino, psycopg2

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient

_ATTR_CONNECT_ARGS = "connect_args"


class <Name>ProxyClient(BaseDbProxyClient):
    """Proxy client for <Name> connections.

    connect_args accepts:
    - dict: serialized to driver connection format (see _serialize below if ODBC)
    - str: passed through unchanged (legacy DC path)
    """

    _DEFAULT_LOGIN_TIMEOUT_IN_SECONDS = 15
    _DEFAULT_QUERY_TIMEOUT_IN_SECONDS = 60 * 14  # 14 minutes

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        super().__init__(connection_type="<connection-type-name>")
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"<Name> agent client requires {_ATTR_CONNECT_ARGS} in credentials"
            )
        connect_args: Union[str, dict] = credentials[_ATTR_CONNECT_ARGS]
        # ... open connection using connect_args ...
        self._connection = <driver>.connect(...)

    @property
    def wrapped_client(self):
        return self._connection
```

### ODBC / pyodbc clients

If using pyodbc (e.g. SQL Server, Azure, MS Fabric), serialize the dict to an ODBC string and add datetimeoffset support:

```python
_DATETIMEOFFSET_SQL_TYPE_CODE = -155

def __init__(self, credentials, **kwargs):
    super().__init__(connection_type="<name>")
    if not credentials or _ATTR_CONNECT_ARGS not in credentials:
        raise ValueError(...)
    connect_args = credentials[_ATTR_CONNECT_ARGS]
    if isinstance(connect_args, dict):
        connection_string = ";".join(
            f"{k}={_odbc_escape(str(v))}" for k, v in connect_args.items()
        )
    elif isinstance(connect_args, str):
        connection_string = connect_args
    else:
        raise ValueError(f"{_ATTR_CONNECT_ARGS} must be a dict or str, got {type(connect_args).__name__}")
    self._connection = pyodbc.connect(
        connection_string,
        timeout=credentials.get("login_timeout", self._DEFAULT_LOGIN_TIMEOUT_IN_SECONDS),
    )
    self._connection.add_output_converter(self._DATETIMEOFFSET_SQL_TYPE_CODE, self._handle_datetimeoffset)
    self._connection.timeout = credentials.get("query_timeout_in_seconds", self._DEFAULT_QUERY_TIMEOUT_IN_SECONDS)
```

**Always include `_odbc_escape` for dict→ODBC serialization** — values containing `;`, `{`, `}`, or `=` must be brace-wrapped:

```python
def _odbc_escape(value: str) -> str:
    if value.startswith("{") and value.endswith("}"):
        return value  # already wrapped (e.g. driver names)
    if any(c in value for c in (";", "{", "}", "=")):
        return "{" + value.replace("}", "}}") + "}"
    return value
```

Also add `_process_description` and `_handle_datetimeoffset` (copy verbatim from `fabric_proxy_client.py`).

### Non-ODBC clients (trino, psycopg2, etc.)

See `starburst_proxy_client.py` (trino) or `postgres_proxy_client.py` (psycopg2) for patterns.
For trino, `connect_args` is a dict of kwargs passed directly to `trino.dbapi.connect(**connect_args)`.

---

## Step 2: CTP Default Config

**File:** `apollo/integrations/ctp/defaults/<name>.py`

**→ Run `/add-ccp-connector` for detailed guidance** on the TypedDict schema, `CtpConfig`/`MapperConfig` structure, Jinja2 template rules, optional fields, transform steps, and SSL cert materialization.

Reference implementation: `apollo/integrations/ctp/defaults/fabric.py`

### ODBC / Azure AD addition not covered by `/add-ccp-connector`

For integrations using ODBC Driver 18 + Azure AD service principal auth, the tenant ID is encoded in the `UID` field — this is ODBC-specific and not in the `/add-ccp-connector` examples:

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
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
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
def test_connect_args_dict_serialized(self, mock_connect): ...

# Happy path — legacy string connect_args passed through
@patch("<driver>.connect")
def test_connect_args_string_passed_through(self, mock_connect): ...

# Error — missing connect_args
def test_missing_connect_args_raises(self): ...

# CTP round-trip — flat creds → resolve → correct connect_args → driver call
def test_ctp_registered(self): ...
def test_ctp_resolves_flat_credentials(self): ...
@patch("<driver>.connect")
def test_ctp_to_proxy_client_end_to_end(self, mock_connect): ...

# CTP bypass — connect_args already present → unchanged
def test_ctp_bypasses_when_connect_args_present(self): ...

# pyodbc only — datetimeoffset handling
def test_handle_datetimeoffset(self): ...
```

For ODBC integrations, also test ODBC value escaping for special characters:

```python
@patch("pyodbc.connect")
def test_dict_value_with_semicolon_is_escaped(self, mock_connect):
    tricky = "p@ss;word=1"
    creds = {**_CONNECT_ARGS_DICT, "PWD": tricky}
    <Name>ProxyClient(credentials={"connect_args": creds}, platform="test")
    call_args = mock_connect.call_args[0][0]
    self.assertIn("PWD={p@ss;word=1}", call_args)
    self.assertNotIn("PWD=p@ss;word=1", call_args)
```

---

## Reference Implementations

| Driver | Proxy client | CTP config |
|--------|-------------|------------|
| pyodbc + AAD (MS Fabric) | `fabric_proxy_client.py` | `ctp/defaults/fabric.py` |
| trino (Starburst) | `starburst_proxy_client.py` | `ctp/defaults/starburst_galaxy.py` |
| psycopg2 (Postgres) | `postgres_proxy_client.py` | `ctp/defaults/postgres.py` |

---

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Bracket notation in template: `raw['port']` | Use `raw.port` |
| Missing `_odbc_escape` for ODBC dict serialization | Copy from `fabric_proxy_client.py` — values with `;`, `{`, `}`, `=` must be brace-wrapped |
| `UID` missing `@tenant_id` for AAD service principal | Use `{{ raw.client_id }}@{{ raw.tenant_id }}` |
| Registering CTP before proxy client supports dict | Wait until Step 1 accepts dict `connect_args` |
| Forgetting `_discover()` import | Without it, `CtpRegistry.get("<name>")` returns `None` and flat creds will fail |
| Adding `| int` / `| float` type filters | Remove — NativeEnvironment preserves Python types automatically |
| Using `connect()` return_value instead of direct attrs in mock cursor | Set `cursor.description` and `cursor.rowcount` as attributes, not `return_value` |
