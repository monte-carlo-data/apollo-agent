# HTTP — Generic HTTP Proxy Client and SSRF Guard

Generic HTTP client infrastructure used by HTTP-based integrations, plus a
server-side request forgery (SSRF) guard that blocks requests to cloud metadata
services, RFC 1918 ranges (where not expected), and other sensitive targets.

## Key files

- **`http_proxy_client.py`** — `HttpProxyClient`: generic HTTP proxy client with
  `do_request`, `download_bytes`, and `download_to_storage` methods used by all
  HTTP-based integrations.
- **`url_safety.py`** — SSRF guard: a `urllib3` connection hook plus the
  `safety_policy` context manager that activates it per-thread.
- **`informatica_proxy_client.py`** — `InformaticaProxyClient`: `HttpProxyClient`
  subclass for Informatica Intelligent Data Management Cloud.
- **`mulesoft_proxy_client.py`** — `MuleSoftProxyClient`: `HttpProxyClient`
  subclass for MuleSoft Anypoint.

## SSRF guard architecture

The guard wraps `urllib3.util.connection.create_connection` **once at module
import time**. The wrapper is transparent by default: if no policy is active on
the current thread, every call passes straight through, so Snowflake, GCS,
Azure, and other SDK-managed connections are completely unaffected.

When a `safety_policy` context manager is active, the wrapper resolves the
destination IP and runs it against the configured block list before allowing
the connection.

### Two tiers

| Tier | Activated by | What it allows |
|---|---|---|
| **Default** | `safe_request(method, url, **kwargs)` | RFC 1918 allowed; metadata service ranges and loopback blocked |
| **Strict** | `safety_policy(url, strict_ip_policy=True, https_only=True)` | Public IPs only + HTTPS required; used for pre-signed URL downloads |

### Redirect handling

Every TCP connection attempt goes through `create_connection`, so each redirect
hop is independently checked. There is no need to subclass `requests.Session` or
intercept the redirect chain at a higher level.

### Operator extension points

| Env var | Default | Effect |
|---|---|---|
| `MCD_HTTP_BLOCKED_CIDRS` | _(empty)_ | Comma-separated extra CIDRs to block |
| `MCD_HTTP_REQUIRE_HTTPS` | `false` | Require HTTPS on the default tier |

Both are read once at import time. Changing them requires a process restart.

## Call-site guidance

**General HTTP requests** — use `safe_request`:

```python
from apollo.integrations.http.url_safety import safe_request

response = safe_request("GET", "https://api.example.com/data", headers={...})
```

**Strict downloads** (pre-signed URLs received from upstream systems) — enter
`safety_policy` explicitly and disable automatic redirects so each hop is
evaluated:

```python
from apollo.integrations.http.url_safety import safety_policy
import requests

with safety_policy(url, strict_ip_policy=True, https_only=True):
    response = requests.get(url, allow_redirects=False)
```

**Do not use `requests.request` directly** — the URL-layer scheme and host
pre-flight checks in `safety_policy` will be skipped (the IP-level hook still
fires at connection time, but the earlier URL validation won't run).

## Threading note

The policy is stored in `threading.local`, so it is scoped to the thread that
entered the context manager. If HTTP work is dispatched to a worker thread
(e.g. `ThreadPoolExecutor`, `asyncio.run_in_executor`), re-enter `safety_policy`
on that worker thread — it does not propagate automatically.

## Out of scope

- **`apollo/integrations/db/`** — DB clients speak DB-specific wire protocols
  (TDS, MySQL binary protocol, etc.) with their own connection security model;
  the HTTP SSRF guard does not apply.
- **`apollo/integrations/ctp/transforms/`** — CTP transforms build credentials
  and resolve OAuth tokens before a connection is opened; they intentionally
  do not route through the guard.
