# CCP Resolution Move to Credentials Layer

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move credential decoding (`decode_dictionary`) and CCP resolution (`CcpRegistry.resolve`) out of `ProxyClientFactory._create_proxy_client` and into `BaseCredentialsService.get_credentials()`, so that CCP transformation happens at the credential extraction layer for all entry points.

**Architecture:** Thread `connection_type` from the two call sites in `main.py` down to `BaseCredentialsService.get_credentials()`. After `_merge_connect_args`, call `decode_dictionary` then `CcpRegistry.resolve(connection_type, merged)`. Remove both calls from `ProxyClientFactory`. Update `PostgresCcpPathTests` to pre-process credentials through `_extract_credentials_in_request` since CCP no longer runs inside the agent call itself.

**Tech Stack:** Python 3.12, `apollo.common.agent.serde.decode_dictionary`, existing CCP registry/pipeline in `apollo/integrations/ccp/`

---

## File Map

**Modified files:**
- `apollo/credentials/base.py` — add `connection_type` param to `get_credentials()`; call decode then CCP after merge
- `apollo/interfaces/generic/main.py` — thread `connection_type` to `_extract_credentials_in_request()` and its 2 call sites
- `apollo/agent/proxy_client_factory.py` — remove `decode_dictionary` call, its import, and the 3 CCP lines from `_create_proxy_client`
- `tests/test_postgres_client.py` — update `PostgresCcpPathTests` to pre-resolve credentials before calling `execute_operation`

**New files:**
- `tests/test_base_credentials_service.py` — tests for the new decode + CCP behavior in `get_credentials()`

---

## Chunk 1: Thread connection_type + add decode/CCP to BaseCredentialsService

### Task 1: Thread connection_type and add decode + CCP to BaseCredentialsService

**Files:**
- Modify: `apollo/credentials/base.py`
- Modify: `apollo/interfaces/generic/main.py:210-212, 339, 1263-1265`
- Create: `tests/test_base_credentials_service.py`

**Background:** `BaseCredentialsService.get_credentials()` currently takes only `credentials: dict`. The two call sites in `main.py` (`execute_agent_operation` and `execute_agent_script`) both have `connection_type` as a parameter but don't pass it down. No existing tests for `BaseCredentialsService` exist.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_base_credentials_service.py
import base64
from unittest import TestCase

from apollo.credentials.base import BaseCredentialsService


class TestBaseCredentialsServiceDecode(TestCase):
    """Verify decode_dictionary runs after _merge_connect_args."""

    def test_plain_credentials_returned_unchanged(self):
        svc = BaseCredentialsService()
        creds = {"connect_args": {"host": "h", "port": 5432}}
        result = svc.get_credentials(creds)
        self.assertEqual({"connect_args": {"host": "h", "port": 5432}}, result)

    def test_binary_value_decoded(self):
        # Simulate a bytes value encoded over the wire as {"__type__": "bytes", "__data__": "..."}
        encoded = {"__type__": "bytes", "__data__": base64.b64encode(b"raw-cert").decode()}
        svc = BaseCredentialsService()
        result = svc.get_credentials({"connect_args": {"cert": encoded}})
        self.assertEqual(b"raw-cert", result["connect_args"]["cert"])


class TestBaseCredentialsServiceCcp(TestCase):
    """Verify CCP runs after decode when connection_type is provided."""

    def test_no_connection_type_skips_ccp(self):
        svc = BaseCredentialsService()
        flat = {"host": "h", "database": "d", "user": "u", "password": "p", "port": 5432}
        result = svc.get_credentials(flat)
        # No connection_type — CCP does not run, flat creds returned unchanged
        self.assertNotIn("connect_args", result)

    def test_postgres_flat_credentials_resolved(self):
        import apollo.integrations.ccp.defaults.postgres  # noqa: F401
        svc = BaseCredentialsService()
        result = svc.get_credentials(
            {
                "host": "db.example.com",
                "port": 5432,
                "database": "mydb",
                "user": "admin",
                "password": "secret",
            },
            connection_type="postgres",
        )
        self.assertIn("connect_args", result)
        self.assertEqual("db.example.com", result["connect_args"]["host"])
        self.assertEqual("mydb", result["connect_args"]["dbname"])
        self.assertEqual("require", result["connect_args"]["sslmode"])
        self.assertNotIn("sslrootcert", result["connect_args"])

    def test_legacy_connect_args_not_overwritten_by_ccp(self):
        import apollo.integrations.ccp.defaults.postgres  # noqa: F401
        svc = BaseCredentialsService()
        # Legacy shape: connect_args already present — CCP is a no-op
        legacy = {"connect_args": {"host": "h", "dbname": "d"}}
        result = svc.get_credentials(legacy, connection_type="postgres")
        self.assertEqual(legacy, result)

    def test_unknown_connection_type_returns_credentials_unchanged(self):
        svc = BaseCredentialsService()
        flat = {"host": "h", "database": "d"}
        result = svc.get_credentials(flat, connection_type="not_a_real_type")
        self.assertEqual(flat, result)
```

- [ ] **Step 2: Run — expect failures**

```bash
cd /path/to/worktree && source ~/.venv/apollo-agent/bin/activate && python -m pytest tests/test_base_credentials_service.py -v
```
Expected: `TypeError: get_credentials() got an unexpected keyword argument 'connection_type'` (or similar import errors).

- [ ] **Step 3: Update `BaseCredentialsService.get_credentials()` in `apollo/credentials/base.py`**

Add the `decode_dictionary` import at the top of the file:
```python
from apollo.common.agent.serde import decode_dictionary
```

Replace `get_credentials`:
```python
def get_credentials(self, credentials: dict, connection_type: str | None = None) -> dict:
    external_credentials = self._load_external_credentials(credentials)
    merged = self._merge_connect_args(
        incoming_credentials=credentials,
        external_credentials=external_credentials,
    )
    merged = decode_dictionary(merged)
    if connection_type:
        import apollo.integrations.ccp.defaults.postgres  # noqa: F401 — triggers registration; TODO: replace with single bootstrap import once more connectors adopt CCP
        from apollo.integrations.ccp.registry import CcpRegistry
        merged = CcpRegistry.resolve(connection_type, merged)
    return merged
```

- [ ] **Step 4: Run — expect all 6 tests PASS**

```bash
source ~/.venv/apollo-agent/bin/activate && python -m pytest tests/test_base_credentials_service.py -v
```

- [ ] **Step 5: Thread `connection_type` through `main.py`**

Update `_extract_credentials_in_request` (line 1263):
```python
def _extract_credentials_in_request(credentials: Dict, connection_type: str | None = None) -> Dict:
    credential_service = CredentialsFactory.get_credentials_service(credentials)
    return credential_service.get_credentials(credentials, connection_type=connection_type)
```

Update the call in `execute_agent_operation` (line 210):
```python
credentials = _extract_credentials_in_request(
    json_request.get("credentials", {}),
    connection_type=connection_type,
)
```

Update the call in `execute_agent_script` (line 339):
```python
credentials = _extract_credentials_in_request(
    json_request.get("credentials", {}),
    connection_type=connection_type,
)
```

> **Note:** `execute_agent_script` (unlike `execute_agent_operation`) has no `try/except` around credential extraction. This is a pre-existing gap — CCP errors will surface as unhandled exceptions here. Adding error handling to match `execute_agent_operation`'s pattern is a judgment call; flag it with the team if this needs to be addressed as part of this task.

> **Note:** `ProxyClientFactory._get_cache_key` hashes the credentials dict. After this change, credentials arrive at `ProxyClientFactory` already resolved to `{"connect_args": {...}}` rather than the original flat shape. The cache key will be computed over the resolved shape, which is semantically correct but means the client cache will be cold on first deploy (existing entries keyed from flat credentials won't match). This is safe — just worth being aware of.

- [ ] **Step 6: Run full test suite — expect all PASS**

```bash
source ~/.venv/apollo-agent/bin/activate && python -m pytest tests/ --tb=short 2>&1 | tail -10
```

- [ ] **Step 7: Commit**

```bash
git add apollo/credentials/base.py apollo/interfaces/generic/main.py tests/test_base_credentials_service.py
git commit -m "feat(ccp): move decode + CCP resolution into BaseCredentialsService.get_credentials"
```

---

### Task 2: Remove decode + CCP from ProxyClientFactory, update PostgresCcpPathTests

**Files:**
- Modify: `apollo/agent/proxy_client_factory.py`
- Modify: `tests/test_postgres_client.py`

**Background:** `ProxyClientFactory._create_proxy_client` currently calls `decode_dictionary` and `CcpRegistry.resolve` — both now handled upstream in `BaseCredentialsService`. `PostgresCcpPathTests.test_ccp_path_resolves_flat_credentials` calls `agent.execute_operation` directly with flat credentials, bypassing `_extract_credentials_in_request`. That test needs to pre-resolve credentials before calling the agent.

- [ ] **Step 1: Run the existing Postgres tests — confirm current baseline**

```bash
source ~/.venv/apollo-agent/bin/activate && python -m pytest tests/test_postgres_client.py -v
```
Expected: all 4 pass.

- [ ] **Step 2: Strip decode + CCP from `_create_proxy_client` in `apollo/agent/proxy_client_factory.py`**

Find `_create_proxy_client` and remove:
1. The `if credentials:` block that wraps the decode/CCP calls (leaving `return factory_method(credentials, platform=platform)` unconditional inside the `if factory_method:` block)
2. The `from apollo.integrations.ccp.registry import CcpRegistry` line
3. The `import apollo.integrations.ccp.defaults.postgres` line
4. The `credentials = CcpRegistry.resolve(...)` line
5. The `credentials = decode_dictionary(credentials)` line

The method should become:
```python
@classmethod
def _create_proxy_client(
    cls, connection_type: str, credentials: Optional[Dict], platform: str
) -> BaseProxyClient:
    factory_method = _CLIENT_FACTORY_MAPPING.get(connection_type)
    if factory_method:
        return factory_method(credentials, platform=platform)
    else:
        raise AgentError(
            f"Connection type not supported by this agent: {connection_type}"
        )
```

Also remove the `decode_dictionary` import at the top of the file:
```python
from apollo.common.agent.serde import decode_dictionary  # DELETE THIS LINE
```

- [ ] **Step 3: Run Postgres tests — expect `PostgresCcpPathTests` to FAIL, legacy tests to PASS**

```bash
source ~/.venv/apollo-agent/bin/activate && python -m pytest tests/test_postgres_client.py -v
```
Expected: `PostgresClientTests` (3 tests) pass, `PostgresCcpPathTests` fails — flat credentials bypass `_extract_credentials_in_request` so CCP no longer runs.

- [ ] **Step 4: Update `PostgresCcpPathTests` in `tests/test_postgres_client.py`**

Add import at the top of the file:
```python
from apollo.interfaces.generic.main import _extract_credentials_in_request
```

In `test_ccp_path_resolves_flat_credentials`, replace the direct flat credentials call with pre-resolved credentials:
```python
@patch("psycopg2.connect")
def test_ccp_path_resolves_flat_credentials(self, mock_connect):
    """Flat credentials are resolved by BaseCredentialsService before reaching PostgresProxyClient."""
    mock_connect.return_value = self._mock_connection
    self._mock_cursor.fetchall.return_value = []
    self._mock_cursor.description.return_value = []
    self._mock_cursor.rowcount.return_value = 0

    operation_dict = {
        "trace_id": "ccp-test",
        "skip_cache": True,
        "commands": [
            {"method": "cursor", "store": "_cursor"},
            {"target": "_cursor", "method": "execute", "args": ["SELECT 1", None]},
            {"target": "_cursor", "method": "fetchall", "store": "tmp_1"},
            {"target": "_cursor", "method": "description", "store": "tmp_2"},
            {"target": "_cursor", "method": "rowcount", "store": "tmp_3"},
            {"target": "__utils", "method": "build_dict",
             "kwargs": {"all_results": {"__reference__": "tmp_1"},
                        "description": {"__reference__": "tmp_2"},
                        "rowcount": {"__reference__": "tmp_3"}}},
        ],
    }
    # CCP now runs in the credentials layer — pre-resolve before calling execute_operation
    resolved = _extract_credentials_in_request(_POSTGRES_FLAT_CREDENTIALS, connection_type="postgres")
    self._agent.execute_operation("postgres", "run_query", operation_dict, resolved)

    mock_connect.assert_called_once()
    call_kwargs = mock_connect.call_args.kwargs
    self.assertEqual("www.test.com", call_kwargs["host"])
    self.assertEqual("u", call_kwargs["user"])
    self.assertEqual("db1", call_kwargs["dbname"])  # CCP mapped database → dbname
    self.assertEqual(1, call_kwargs["keepalives"])
```

- [ ] **Step 5: Run all Postgres tests — expect all 4 PASS**

```bash
source ~/.venv/apollo-agent/bin/activate && python -m pytest tests/test_postgres_client.py -v
```

- [ ] **Step 6: Run full test suite — expect all PASS, no regressions**

```bash
source ~/.venv/apollo-agent/bin/activate && python -m pytest tests/ --tb=short 2>&1 | tail -10
```

- [ ] **Step 7: Commit**

```bash
git add apollo/agent/proxy_client_factory.py tests/test_postgres_client.py
git commit -m "refactor(ccp): remove decode + CCP from ProxyClientFactory; update CCP integration test"
```

---

## What's next (out of scope for this plan)

- **Bootstrap consolidation** — replace the bare `import apollo.integrations.ccp.defaults.postgres` in `base.py` with a single `import apollo.integrations.ccp.defaults` once Snowflake and Redshift adopt CCP
- **`decode_dictionary` as a CCP transform** — a `decode_bytes` primitive that makes decoding explicit in the pipeline rather than implicit in the credentials service
- **Snowflake + Redshift migration** — add CCP configs for those connectors; the credentials layer will automatically handle them once registered
