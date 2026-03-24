# Research Subagent Prompt Templates

---

## Web Research Agent Prompt

Use this as the complete prompt when dispatching the Web Research Agent.
Replace `{{INTEGRATION_NAME}}` and `{{USER_NOTES_LINE}}` before dispatching.

---

You are researching the **{{INTEGRATION_NAME}}** data source for a Monte Carlo integration.
{{USER_NOTES_LINE}}
(If user notes were provided, replace {{USER_NOTES_LINE}} with "Additional context: <notes>". Otherwise omit the line.)

Research the following and return structured findings. Be specific — cite source URLs and
exact API/view names where found. If you cannot find something, say so explicitly rather
than guessing.

**Research areas:**

1. **Drivers and libraries**: What Python libraries, JDBC drivers, or REST APIs are
   available to connect to {{INTEGRATION_NAME}}? Include package names and versions.

2. **Authentication methods**: What auth options does the vendor support?
   (OAuth 2.0, API key, username/password, service accounts, certificates, etc.)
   Note any that are preferred or required for production use.

3. **SSL/TLS**: What certificate or encryption requirements exist?
   Are certificates provided by the vendor or self-managed?

4. **Metadata sources**: For each metadata type below, find the exact system view,
   table, API endpoint, or SQL query that can provide it:
   - Tables (list of tables, schemas, databases)
   - Columns (column names, types, nullable)
   - Query logs (executed queries, user, timestamp, duration)
   - Lineage (if the vendor exposes lineage or dependency info)
   - Volume (row counts or approximate table sizes)
   - Freshness (last modified time for tables)

5. **Known limitations**: Rate limits, row count caps, pagination requirements,
   permission requirements, anything affecting large-scale metadata collection.

6. **Private Link support**: Does the vendor support AWS PrivateLink? Azure Private Link?
   Cite the vendor docs page if yes.

7. **Existing Monte Carlo docs**: Search for any existing Monte Carlo documentation,
   blog posts, or customer-facing references to {{INTEGRATION_NAME}}.

**Return your findings as a JSON object:**

```json
{
  "drivers": [
    {"name": "...", "type": "python|jdbc|rest", "package": "...", "notes": "..."}
  ],
  "auth_methods": [
    {"method": "...", "notes": "...", "preferred": true}
  ],
  "ssl_requirements": "...",
  "metadata_sources": {
    "tables":     {"source": "...", "example_query": "...", "notes": "..."},
    "columns":    {"source": "...", "example_query": "...", "notes": "..."},
    "query_logs": {"source": "...", "example_query": "...", "notes": "..."},
    "lineage":    {"source": "...", "notes": "...", "available": true},
    "volume":     {"source": "...", "example_query": "...", "notes": "..."},
    "freshness":  {"source": "...", "example_query": "...", "notes": "..."}
  },
  "known_limitations": ["..."],
  "private_link_support": {"aws": true, "azure": false, "notes": "..."},
  "monte_carlo_existing_docs": "url or null",
  "sources_consulted": ["url1", "url2"]
}
```

---

## Codebase Pattern Agent Prompt

Use this as the complete prompt when dispatching the Codebase Pattern Agent.
Replace `{{INTEGRATION_NAME}}` and path constants before dispatching.

---

You are finding the best analogous existing integrations in the Monte Carlo codebase
for a new **{{INTEGRATION_NAME}}** integration.

Search these directories:
- Apollo agent integrations: `{{APOLLO_AGENT_ROOT}}/apollo/integrations/`
- Data collector: `{{DATA_COLLECTOR_ROOT}}/`
- Monolith: `{{MONOLITH_ROOT}}/`
- Frontend: `{{FRONTEND_ROOT}}/`
- Saas-serverless normalizers: `{{SAAS_SERVERLESS_ROOT}}/datapipeline/normalizers/`

**Your goal:** Find the 1–2 existing integrations most similar to {{INTEGRATION_NAME}}
(consider: same protocol type, similar auth, similar metadata extraction approach).

For each analog integration, extract:

1. **Proxy client** (apollo-agent):
   - File path and class name
   - `__init__` signature
   - CCP config path (if one exists under `apollo/integrations/ccp/defaults/`)
   - Credential shape (what keys does connect_args contain?)

2. **DC plugin** (data-collector):
   - File path
   - How metadata is extracted (query-based? API? special driver?)
   - Credential model class and fields

3. **Monolith models**:
   - Connection model class and file path
   - Warehouse model class if applicable
   - Key fields on these models

4. **Multi-instance topology** (for SQL-based integrations):
   Note how the analog handles scenarios where a customer has multiple engine or
   warehouse instances, and how connections are scoped (one connection = one engine?
   one connection = one account with multiple warehouses?).

5. **Frontend** (connection onboarding UI):
   - Find the connection form or wizard component for the analog integration
   - Note which credential fields are rendered, their types (text, password, select),
     and any conditional/optional fields
   - Note any integration-specific help text or validation patterns

6. **Normalizers** (saas-serverless):
   - Find the normalizer for the analog integration under `datapipeline/normalizers/`
   - Note the normalizer class, which raw fields it maps, and any transformation logic
     that would differ for a new integration (e.g., custom timestamp parsing, type coercion)

**If a configured path does not exist**, note it in `unreachable_repos` and continue.

**Return your findings as a JSON object:**

```json
{
  "analog_integrations": [
    {
      "name": "...",
      "similarity_reason": "...",
      "proxy_client": {
        "path": "...",
        "class_name": "...",
        "init_signature": "...",
        "ccp_config_path": "... or null",
        "credential_shape": {"key": "type"}
      },
      "dc_plugin": {
        "path": "...",
        "metadata_approach": "...",
        "credential_model": "..."
      },
      "monolith": {
        "connection_model_path": "...",
        "warehouse_model_path": "... or null",
        "key_fields": ["..."]
      },
      "frontend": {
        "component_path": "... or null",
        "credential_fields": [{"name": "...", "type": "text|password|select", "required": true}],
        "conditional_fields": ["..."],
        "notes": "..."
      },
      "normalizer": {
        "path": "... or null",
        "class_name": "...",
        "mapped_fields": ["..."],
        "notable_transforms": ["..."]
      },
      "notable_patterns": ["..."]
    }
  ],
  "reusable_patterns": ["..."],
  "structural_gaps": ["..."],
  "unreachable_repos": []
}
```

---

## Prototype Agent Prompt

Use this as the complete prompt when dispatching the Prototype Agent.
Replace all `{{VARIABLE}}` placeholders before dispatching.

---

You are building a feasibility prototype for a new **{{INTEGRATION_NAME}}** integration
in the Monte Carlo apollo-agent.

**Web research findings:**
```json
{{WEB_FINDINGS_JSON}}
```

**Codebase pattern findings (closest analog):**
```json
{{CODEBASE_FINDINGS_JSON}}
```

**Credentials:** {{CREDENTIALS_OR_NULL}}
(null means skip live tests — write code conceptually only)

**Output directory:** `{{APOLLO_AGENT_ROOT}}/docs/superpowers/prototypes/`
**Output filename:** `{{DATE}}-{{INTEGRATION_SLUG}}-prototype.py`

---

### Your tasks

#### 1. Write the prototype client

Create the prototype file at the path above.

Write a minimal `{{IntegrationName}}ProxyClient` class following the closest analog's pattern.
Use the driver identified in web research findings.

The file must contain:
- Import statements
- A minimal class with `__init__(self, connect_args: dict)`
- A `test_connection()` method that opens and closes a connection
- A method for each metadata query: `get_tables()`, `get_columns(table)`,
  `get_query_logs()`, `get_volume(table)`, `get_freshness(table)`
- A `__main__` block that runs all tests and prints results

Use the web research's `example_query` fields as the basis for each method's SQL.
Follow the analog proxy client's patterns exactly where possible.

#### 2. Live connection test (skip everything if credentials is null)

If credentials were provided, attempt a live connection using them.
Record success or failure and exact error if failed.

#### 3. Metadata query tests (skip if connection failed)

Run each metadata method and record:
- The exact SQL or API call used
- Status: success / failed / skipped
- Sample output: first 3–5 rows (or the error message if failed)

#### 4. Return findings

Return a JSON object AND confirm the prototype file was written:

```json
{
  "connection_status": "success|failed|skipped",
  "connection_error": "... or null",
  "prototype_file_path": "...",
  "credential_shape_validated": {"key": "type"},
  "driver_used": "...",
  "metadata_query_results": {
    "tables":     {"query": "...", "status": "success|failed|skipped", "sample": [], "error": null},
    "columns":    {"query": "...", "status": "success|failed|skipped", "sample": [], "error": null},
    "query_logs": {"query": "...", "status": "success|failed|skipped", "sample": [], "error": null},
    "volume":     {"query": "...", "status": "success|failed|skipped", "sample": [], "error": null},
    "freshness":  {"query": "...", "status": "success|failed|skipped", "sample": [], "error": null}
  },
  "issues_discovered": ["..."]
}
```
