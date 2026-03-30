---
skill: code-review
phase: fixes-verified
sha: caaeedf
instruction: |
  This is a code-review report. All findings are pre-checked (opt-out model).
  If the user says "go", "fix", or similar — FIRST READ the code-review skill
  (find it at `~/.claude/skills/code-review/SKILL.md`), then execute
  Phase 4 (Fix) for all [x] checked findings. Skip any unchecked [ ] ones.
  User **Instructions** on each finding take priority over reviewer suggestions.
---

# Code Review: aroitman/fabric-integration

> Adds `microsoft-fabric` proxy client with CTP integration, enabling Monte Carlo agents to connect to Microsoft Fabric SQL Warehouse via ODBC with Azure AD service principal auth.
> Reviewed: 2026-03-30 | Reviewers: security, devdocs, correctness, testing | Scope: full PR

## Summary

- 0 blockers, 2 issues, 2 suggestions, 1 nit
- Overall this is a solid, well-scoped integration. The code follows established pyodbc client patterns closely, the CTP round-trip is correctly wired, and the test coverage is thorough. Two real issues to address: ODBC special-character escaping in dict serialization, and missing CLAUDE.md for these module directories.

## Findings

### Blockers

_None._

### Issues

- [verified] **F1. [ISSUE] Dict-to-ODBC-string serialization does not escape special characters**
- **File:** `apollo/integrations/db/fabric_proxy_client.py:L43`
- **Reviewer:** correctness, security
- **Description:** The dict serialization `";".join(f"{k}={v}" for k, v in connect_args.items())` does not escape values containing semicolons or equals signs. ODBC connection string values that contain special characters (`;`, `{`, `}`) must be wrapped in curly braces (`{value}`). For example, if `PWD` contains a semicolon (e.g. a password like `"p@ss;word=1"`), the resulting string `PWD=p@ss;word=1` injects a spurious `word=1` key-value pair, malforming the connection string or silently connecting with wrong credentials.
- **Why it matters:** Connection failures or silent auth bypass when credentials contain special characters. Azure AD service principal secrets frequently contain `+`, `=`, and `/` chars; semicolons are less common but possible.
- **Suggestion:** Wrap values that contain special chars in `{...}`:
  ```python
  def _odbc_escape(value: str) -> str:
      # ODBC spec: wrap in braces if value contains ; { } or =
      if any(c in value for c in (";", "{", "}", "=")):
          return "{" + value.replace("}", "}}") + "}"
      return value

  connection_string = ";".join(
      f"{k}={_odbc_escape(str(v))}" for k, v in connect_args.items()
  )
  ```
  Note: the sibling `AzureDatabaseProxyClient` only accepts a pre-built string from the DC and never serializes a dict, so this is a new concern unique to `MsFabricProxyClient`.
- **Confidence:** high
- **Instructions:**

- [verified] **F2. [ISSUE] No CLAUDE.md in ctp/ or db/ integration directories**
- **File:** `apollo/integrations/ctp/`, `apollo/integrations/db/`
- **Reviewer:** devdocs
- **Description:** Both `apollo/integrations/ctp/` (8+ source files, non-trivial CTP pipeline machinery) and `apollo/integrations/db/` (16+ source files, multiple proxy client classes) have no `CLAUDE.md`. Per the devdocs reviewer rules, directories with meaningful logic and 3+ source files should have a `CLAUDE.md` to orient developers and AI agents.
- **Why it matters:** The CTP pipeline in particular has non-obvious conventions: what `_discover()` does, the "Phase 2" migration plan referenced in comments, when to register a new CTP vs leave it unregistered. Without orientation docs, these patterns must be reverse-engineered.
- **Suggestion:** Add `CLAUDE.md` to both directories. For `ctp/`: explain the pipeline stages, when/how to add a new connector, and the Phase 2 plan. For `db/`: explain the base class pattern, how pyodbc clients differ from non-pyodbc ones, and the `connect_args` convention.
- **Confidence:** high
- **Instructions:**

### Suggestions

- [deferred] **F3. [SUGGESTION] `_handle_datetimeoffset` is duplicated across three pyodbc clients**
- **File:** `apollo/integrations/db/fabric_proxy_client.py:L65-L75`
- **Reviewer:** correctness
- **Description:** `_handle_datetimeoffset` is copy-pasted identically from `AzureDatabaseProxyClient` and `SqlServerProxyClient`. Same for `_process_description`, `_DATETIMEOFFSET_SQL_TYPE_CODE`, and the default timeout constants. The pattern is consistent but the duplication means any future fix (e.g., a timezone offset edge case) must be applied in three places.
- **Why it matters:** DRY violation — maintenance burden if the shared logic ever needs changing.
- **Suggestion:** Extract shared pyodbc functionality to a `BasePyodbcProxyClient` in the `db/` package that all three clients inherit from. This is a larger refactor; alternatively, at minimum extract `_handle_datetimeoffset` as a module-level utility in a shared `pyodbc_utils.py`.
- **Confidence:** high
- **Open question:** Is a refactor of all three clients in scope for this PR, or should this be a follow-up?
- **Instructions:**

- [verified] **F4. [SUGGESTION] No test for `pyodbc.connect` raising (connection failure path)**
- **File:** `tests/test_ms_fabric_client.py`
- **Reviewer:** testing
- **Description:** The tests cover credential validation errors but not connection-time failures (e.g., `pyodbc.connect` raises `pyodbc.OperationalError` — network unreachable, wrong server, invalid credentials). There's no test verifying the exception propagates correctly rather than being swallowed.
- **Why it matters:** If exception handling were accidentally added around `pyodbc.connect` in the future, there would be no test to catch a silent failure.
- **Suggestion:** Add a test:
  ```python
  @patch("pyodbc.connect")
  def test_connect_failure_propagates(self, mock_connect):
      """pyodbc connection errors propagate to the caller."""
      import pyodbc
      mock_connect.side_effect = pyodbc.OperationalError("connection refused")
      with self.assertRaises(pyodbc.OperationalError):
          MsFabricProxyClient(credentials={"connect_args": _CONNECT_ARGS_DICT}, platform="test")
  ```
- **Confidence:** medium
- **Instructions:**

### Nits

- [verified] **F5. [NIT] `_mock_cursor.description` and `_mock_cursor.rowcount` set as `return_value` but accessed as attributes in real usage**
- **File:** `tests/test_ms_fabric_client.py:L143-L145`
- **Reviewer:** testing
- **Description:** In `_test_run_query`, the mock sets `self._mock_cursor.description.return_value = description` and `self._mock_cursor.rowcount.return_value = len(data)`. In real pyodbc, `cursor.description` and `cursor.rowcount` are attributes, not callables. The Agent accesses them via `method: "description"` and `method: "rowcount"` in the command list, which the agent framework calls as attribute accesses (not function calls). The `Mock()` will still work here because `Mock().description` returns a `Mock` and `Mock().description.return_value` is accessible, but it creates a subtle mismatch that could mask bugs if the agent framework's attribute access behavior changed.
- **Suggestion:** Set them as direct attributes: `self._mock_cursor.description = description` and `self._mock_cursor.rowcount = len(data)`.
- **Confidence:** medium
- **Instructions:**

## Informational Notes

- **[NOTE]** `fabric_proxy_client.py` — The connection is established in `__init__` and closed in `__del__` via `BaseDbProxyClient`. This means if `add_output_converter` raises after `pyodbc.connect` succeeds, the connection leaks until GC. This is the same pattern in all sibling clients, so not flagging as a new issue for this PR.
- **[NOTE]** `fabric.py` (CTP defaults) — The CTP has an empty `steps=[]` list. This is correct for a simple field-mapping-only connector, consistent with how `sql_server.py` is structured.
- **[NOTE]** `registry.py` — The `_discover()` function now has a side-effect import that registers the Fabric CTP. Adding future connectors requires editing this file. This is the intended pattern per the existing comment ("Add new connector imports here").
- **[NOTE]** `proxy_client_factory.py` — The `# type: ignore` on the `**kwargs` line is consistent with all other factory functions in this file. No action needed.

## Review Notes

