# MCP — Generic Remote MCP Client

Generic client for remote MCP servers (streamable HTTP only). It runs `tools/list`
and `tools/call` and contains no server-specific logic. User-facing behaviour is
documented in the README ("MCP servers").

## Key files

- **`mcp_proxy_client.py`** — `McpProxyClient`: parses config only; per-call
  checks, auth, and `run_operation`. Also `log_payload` redaction.
- **`allowlist.py`** — `MCD_MCP_ALLOWED_HOSTS` + built-in `aws-mcp.*.api.aws`;
  `check_server_url` (https, no embedded credentials, host match).
- **`auth.py`** — `resolve_auth`: `none`, `secret_header`, `aws_sigv4`,
  `oauth_client_credentials`; `SigV4HttpxAuth`; `aws_role_session_name`.
- **`session.py`** — `run_operation`: one `anyio.run` per call, session
  start/resume/close, `_streamable_http_streams`, `_PassthroughClientSession`.
- **`results.py`** — conversion to JSON-safe dicts and the `max_result_bytes` cap.
- **`errors.py`** — `McpErrorCode`, `McpClientError`, `map_exception`.

## Request flow

`/api/v1/agent/execute/mcp/<op>` -> `McpProxyClient` (parses config only) -> per
call: transport check (`streamable_http` only) -> `check_server_url` allowlist ->
`assert_safe_destination` -> `resolve_auth` -> `run_operation` (one `anyio.run`
per call) -> `results` conversion + cap.

## Invariants

- **`aws_sigv4` requires `assumable_role`, never signs with the agent's own
  credentials, and only targets `aws-mcp.<region>.api.aws`.** The execution role
  can assume every tagged role and read the agent bucket, and `aws___run_script`
  runs model-written code with the signing credentials.
- **`RoleSessionName` is stable per role (`aws_role_session_name`).** The AWS MCP
  Server binds sessions to role ARN + RoleSessionName, so resuming with freshly
  assumed credentials depends on it.
- **The factory's 60 s client cache holds parsed config only** — no assumed
  credentials, OAuth tokens, or live sessions. Auth runs per call.
- **`allows_result_location` stays `False`.** MCP results (e.g. customer log
  lines) must never be written to the agent bucket for a pre-signed URL; the size
  cap only bounds them.
- **Logs stay free of model-written content.** `log_payload` redacts tool
  `arguments`/`args`/`kwargs`, and the SDK's `mcp.client.streamable_http` logger
  is pinned above DEBUG so debug mode can't log full JSON-RPC messages.
- **Errors go through `errors.map_exception`**, which unwraps anyio exception
  groups into `McpErrorCode` values reported as `__mcd_error_type__`. Unknown-session
  errors (AWS `-30001`, spec 404/`32600`) map to `session_expired` only on resumed
  calls, which triggers one fresh session.
- **`limits.timeout_seconds` bounds the whole call, including auth** (STS
  AssumeRole / OAuth token fetch).

## When upgrading `mcp`

- `session.py::_streamable_http_streams` is a copy of the SDK's
  `mcp.client.streamable_http.streamable_http_client`; the only difference is that
  it seeds `session_id`/`protocol_version`. Re-diff it against the new SDK.
- `_PassthroughClientSession` overrides the private
  `ClientSession._validate_tool_result` — confirm it still exists.
- Re-check that the transport still reports some failures by sending Exceptions
  into the read stream (handled by the session's message handler).
- 2.x moved to httpx2 and is a bigger port.

## Tests

`tests/test_mcp_*.py`. An in-memory server covers protocol paths; `_FakeHttpServer`
+ `httpx.MockTransport` cover HTTP-level behaviour (resume, DELETE, signing).
