# Integration Research Agent — Design Spec

**Date:** 2026-03-23
**Author:** swaller
**Status:** Draft

---

## Overview

A Claude skill (`/research-integration`) that researches a new Monte Carlo integration from a named data source, fills out a complete SDD using the standard Integration SDD Template, and publishes it as a Notion page. The skill combines autonomous web research, codebase pattern extraction, and a single human-in-the-loop checkpoint to produce a high-quality, realistic integration design document.

**Invocation:**
```
/research-integration <source-name> [-- <optional notes>]
```

Examples:
```
/research-integration Firebolt
/research-integration "Oracle Analytics Cloud" -- customer uses OAuth only, no JDBC
/research-integration Teradata -- self-hosted, private link required
```

**Output:** A Notion SDD page (status: Draft) created as a child of the SDDs parent page, with the URL returned to the user on completion.

**SDD template:** The Notion template is the authoritative source for section structure. The section list in this spec is illustrative; the template governs what sections appear and in what order.

---

## Architecture

### Component Overview

```
/research-integration <name> [-- optional notes]
        │
        ▼
 ┌─────────────────────────────────────────────────────┐
 │  Skill (orchestrator prompt)                        │
 │  • Parses input                                     │
 │  • Fetches Notion SDD template (initialization)     │
 │  • Asks user for credentials (optional)             │
 │  • Dispatches 2 parallel research subagents         │
 └─────────────┬───────────────────┬───────────────────┘
               │                   │
               ▼                   ▼
     ┌──────────────┐   ┌──────────────────┐
     │  Web Research │   │ Codebase Pattern │
     │  Agent        │   │ Agent            │
     └──────┬───────┘   └────────┬─────────┘
            └────────────────────┘
                        │
                        ▼
           ┌────────────────────────┐
           │  Prototype Agent       │
           │  write client code +   │
           │  live connection test  │
           │  + metadata queries    │
           └──────────┬─────────────┘
                      │
                      ▼
           ┌────────────────────────┐
           │   Synthesis Agent      │
           │  distill → Q&A →       │
           │  compile briefing      │
           └──────────┬─────────────┘
                      │
                      ▼
           ┌────────────────────────┐
           │   SDD Writer Agent     │
           │  section-by-section →  │
           │  Notion page           │
           └────────────────────────┘
```

### Design Principles

- **Stateless workers, stateful orchestrator.** Research subagents are disposable and return structured findings. The synthesis agent owns the complete picture from the moment research completes.
- **One human checkpoint.** After research, before writing. Targeted questions only — things the agent genuinely could not determine. Maximum 5 questions per run; questions that would change the tier estimate or affect connection/auth take priority. Remaining gaps are marked `[NEEDS REVIEW]` in the final document rather than held for more questions.
- **One writer.** A single SDD writer agent (not parallel section writers) ensures cross-section coherence. Decisions made in "Connection & Authentication" carry naturally into "Normalization" and "Rollout Estimates."
- **Structured subagent output.** All subagents return structured findings (not prose) so the synthesis agent can distill them compactly and pass complete briefings to follow-up agents if needed.
- **Rigid skill.** This skill must be followed exactly — the template-first initialization, parallel research dispatch, and single human checkpoint are load-bearing. "Rigid" means: do not adapt away from the defined flow, skip phases, or inline work that is assigned to a subagent. See [Skill Types in the superpowers guide](../../../README.md) for the rigid vs. flexible distinction.

---

## Initialization: Fetch SDD Template

**Before any research subagents are launched**, the skill fetches the Notion SDD template:

- **Template URL:** `NOTION_SDD_TEMPLATE_URL` (configured constant — see Skill File section)
- **SDDs parent page:** The parent page under which new SDD pages are created. Its ID is `NOTION_SDD_PARENT_PAGE_ID` (configured constant — see Skill File section).

**Failure behavior:** If the template fetch fails (API error, auth failure, page unavailable), the skill halts immediately with a clear error message. Do not proceed to research with no template — the writer agent cannot produce a valid SDD without it.

The fetched template structure (sections, table schemas, section prompts) is held by the orchestrator and included in the writer briefing compiled in Phase 2 Step 4.

**Credential collection:** After the template fetch, the skill asks the user for connection credentials before dispatching research subagents. Credentials can also be passed inline: `/research-integration Firebolt -- host=foo user=bar password=xxx`. If the user has no test credentials, they can skip — the prototype agent will write client code conceptually and mark all live test results `[PROTOTYPE NOT TESTED]`.

---

## Phase 1: Parallel Research Subagents

Two subagents are dispatched simultaneously after the template is successfully fetched.

### Agent 1: Web Research Agent

**Input:** Integration name + any optional notes from invocation.

**Researches:**
- Available drivers and libraries (JDBC, ODBC, Python native, REST API)
- Authentication methods (OAuth, API key, username/password, service accounts, certificates)
- SSL/TLS and certificate requirements
- System catalog views or API endpoints available for: tables, columns, query logs, lineage, volume, freshness metadata
- Known limitations: rate limits, row count caps, pagination requirements
- Private Link support (AWS and Azure)
- Any existing Monte Carlo public documentation or customer-facing references

**Returns structured findings:**
```
{
  "drivers": [...],
  "auth_methods": [...],
  "ssl_requirements": "...",
  "metadata_sources": {
    "tables": {"source": "...", "notes": "..."},
    "columns": {"source": "...", "notes": "..."},
    "query_logs": {"source": "...", "notes": "..."},
    "lineage": {"source": "...", "notes": "..."},
    "volume": {"source": "...", "notes": "..."},
    "freshness": {"source": "...", "notes": "..."}
  },
  "known_limitations": [...],
  "private_link_support": {"aws": bool, "azure": bool, "notes": "..."},
  "monte_carlo_existing_docs": "..."
}
```

### Agent 2: Codebase Pattern Agent

**Input:** Integration name. Searches the following canonical absolute paths (configured in skill file):
- `<APOLLO_AGENT_ROOT>/apollo/integrations/` — proxy client structure, CCP config, credential shape, transport handling
- `<DATA_COLLECTOR_ROOT>/` — DC plugin structure, metadata extraction implementation for analogous sources, credential model shape
- `<MONOLITH_ROOT>/` — connection model fields, warehouse model, GraphQL mutations for onboarding

**Failure behavior:** If a configured path does not resolve, the agent proceeds with degraded output, explicitly flagging which repos were unreachable in the `unreachable_repos` field. The synthesis agent **must notify the user at the Q&A checkpoint** (Step 2) if any repo was unreachable — before the user answers questions — so they can decide whether to proceed or fix the path configuration first. Unreachable repos also produce `[NEEDS REVIEW]` markers in the final document.

**Identifies:** The 1–2 closest analogous existing integrations and extracts:
- Proxy client structure and credential shape
- CCP config pattern (if one exists for the analog)
- DC plugin layout and metadata extraction approach
- Monolith connection/warehouse model fields

**Returns structured findings:**
```
{
  "analog_integrations": [
    {
      "name": "...",
      "similarity_reason": "...",
      "proxy_client_path": "...",
      "ccp_config_path": "...",
      "dc_plugin_path": "...",
      "monolith_model_path": "...",
      "credential_shape": {...},
      "notable_patterns": [...]
    }
  ],
  "reusable_patterns": [...],
  "structural_gaps": [...],
  "unreachable_repos": [...]
}
```

---

## Phase 1.5: Prototype Agent

Runs sequentially after Phase 1 (needs driver and auth findings from web research), before synthesis. Builds a minimal working client and validates connectivity and metadata queries against the real data source.

**Input:** Web research findings (drivers, auth, metadata sources) + codebase findings (analog proxy client structure) + credentials (if provided).

**Outputs:**
1. A prototype client file written to `<APOLLO_AGENT_ROOT>/docs/superpowers/prototypes/YYYY-MM-DD-<name>-prototype.py`
2. Structured test results

**If no credentials provided:** Writes the client code conceptually (following the closest analog pattern), marks all live test results `[PROTOTYPE NOT TESTED]`, and returns immediately.

**If credentials provided, the agent:**
1. Writes a minimal `<Name>ProxyClient` class following the closest analog's pattern (e.g., `DatabaseProxyClient` subclass for SQL sources), using the driver identified by web research
2. Attempts live connection using provided credentials
3. Runs each proposed metadata query from web research findings:
   - Tables (list of accessible tables/schemas)
   - Columns (column metadata for a sample table)
   - Query logs (if a source was identified)
   - Volume (row count for a sample table)
   - Freshness (last modified time for a sample table)
4. For each query: records the exact SQL/API call used, whether it succeeded, a sample of the result (first 3–5 rows), and any error message

**Returns structured findings:**
```json
{
  "connection_status": "success|failed|skipped",
  "connection_error": "... or null",
  "prototype_file_path": "...",
  "credential_shape_validated": {"key": "type"},
  "metadata_query_results": {
    "tables":    {"query": "...", "status": "success|failed|skipped", "sample": [...], "error": "..."},
    "columns":   {"query": "...", "status": "success|failed|skipped", "sample": [...], "error": "..."},
    "query_logs":{"query": "...", "status": "success|failed|skipped", "sample": [...], "error": "..."},
    "volume":    {"query": "...", "status": "success|failed|skipped", "sample": [...], "error": "..."},
    "freshness": {"query": "...", "status": "success|failed|skipped", "sample": [...], "error": "..."}
  },
  "issues_discovered": ["..."],
  "driver_used": "..."
}
```

**Failure behavior:** If connection fails (wrong credentials, network error, unsupported driver), the agent records the error, marks all query results as `failed`, and returns. The synthesis agent surfaces the failure to the user at the Q&A checkpoint rather than halting.

---

## Phase 2: Synthesis Agent

Receives findings from both research agents, the prototype agent results, and the template structure fetched during initialization. Owns the complete context from this point forward.

### Step 1: Distill

Compresses research into a working brief. Resolves conflicts between web findings and codebase patterns using the following priority rule:

- **Web research governs** findings about the external system (what the vendor supports, what APIs exist, what the auth flow looks like).
- **Codebase patterns govern** findings about Monte Carlo's implementation approach (how to structure the proxy client, what CCP config shape to use, what monolith model fields are needed).
- **Conflicts that span both** (e.g., a vendor auth mechanism that no existing analog uses) are not silently resolved — they are surfaced as targeted questions in Step 2.

### Step 2: Surface findings + targeted Q&A

Presents a human-readable summary to the user:
- What it found: connection options, auth methods, available metadata sources, nearest analog integration
- Prototype results summary: which metadata queries succeeded/failed, any connection issues
- Estimated integration tier (Bronze/Silver/Gold/Platinum) based on confirmed (not just documented) metadata availability
- A short list of **targeted questions** — maximum 5, prioritized as follows:
  1. Questions that would change the tier estimate
  2. Questions that affect connection/auth (the most implementation-critical section)
  3. Questions that affect data extraction coverage
  4. Everything else → marked `[NEEDS REVIEW]` in the document instead

This is the **single human checkpoint**. Once the user responds, the skill proceeds to Step 3 or Step 4 — it does not loop back for more questions.

### Step 3: Handle follow-up research (one round maximum)

If human feedback reveals something that requires additional investigation (e.g., "the customer uses a custom auth mechanism not in the public docs"), the synthesis agent dispatches **one** targeted follow-up subagent with a complete briefing:
- Original research findings (both agents)
- Human feedback verbatim
- The specific question to investigate

After the follow-up subagent returns, the synthesis agent proceeds to Step 4 regardless of whether the follow-up was conclusive. Any remaining unknowns become `[NEEDS REVIEW]` markers. There is no second follow-up round.

### Step 4: Compile writer briefing

Assembles the complete structured document passed to the writer agent:

```
{
  "integration_name": "...",
  "user_notes": "...",
  "estimated_tier": "Bronze|Silver|Gold|Platinum",
  "tier_rationale": "...",
  "web_findings": { ...Agent 1 output... },
  "codebase_findings": { ...Agent 2 output... },
  "prototype_results": { ...Prototype Agent output... },
  "human_answers": [
    {"question": "...", "answer": "..."}
  ],
  "follow_up_findings": { ...follow-up agent output, or null... },
  "template_structure": { ...sections, table schemas, prompts from initialization... },
  "analog_integrations": [...],
  "unresolved_gaps": [...]
}
```

This briefing is the **sole input** to the writer agent.

---

## Phase 3: SDD Writer Agent

Receives the complete writer briefing. Produces the finished SDD in Notion.

### Writing approach

Works through SDD template sections in order, using each prior section as context for the next. The Notion template is authoritative for section list and order — the writer follows the template structure, not a hardcoded list.

For each section:
- Fills content with concrete, specific findings — no placeholder text
- Populates structured tables (data extraction, component summary, design decisions, integration tiers) with real data
- Marks uncertain or unresolved content with `[NEEDS REVIEW: <reason>]` rather than guessing
- Uses prototype results as ground truth: confirmed queries get real example SQL and sample output; failed queries are noted accurately rather than assumed to work
- Cross-references prior sections where relevant (e.g., normalization references extraction sources identified earlier)
- Includes the prototype client code path in the Architecture section as a starting point reference

### Effort and tier estimation

Derives tier classification from:
- Which metadata types are accessible (tables/columns/query logs/lineage)
- How accessible those sources are (native API vs. fragile scraping vs. unavailable)
- Auth and credential complexity relative to existing integrations
- Whether a new proxy client type is needed or an existing one can be extended

Estimates expressed as ranges with explicit assumptions stated, not point estimates.

### Notion output

- Creates a new Notion page as a child of the SDDs parent page (ID from skill file constant)
- Uses the fetched template structure
- Sets: Author = the value of `NOTION_SDD_AUTHOR` (configured constant in skill file — set to your name at install time), Status = "Draft", PRD/Feasibility links left blank
- Returns the direct Notion URL as the final message to the user

---

## Skill File

**Location:** `~/.claude/skills/research-integration.md`

**Skill type:** Rigid — the template-first initialization, parallel dispatch pattern, and single human checkpoint are load-bearing and must not be adapted away.

**Required constants (configured at install time):**
- `NOTION_SDD_TEMPLATE_URL` — `https://www.notion.so/montecarlodata/Integration-SDD-Template-2aa334399e65801c8b5fe48f1448b22d`
- `NOTION_SDD_PARENT_PAGE_ID` — The Notion page ID of the parent page under which new SDD drafts are created
- `NOTION_SDD_AUTHOR` — Your name as it should appear in the SDD Author field (e.g. "swaller")
- `APOLLO_AGENT_ROOT` — Absolute path to the local apollo-agent repo checkout
- `DATA_COLLECTOR_ROOT` — Absolute path to the local data-collector repo checkout
- `MONOLITH_ROOT` — Absolute path to the local monolith-django repo checkout

Prototype files are written to `<APOLLO_AGENT_ROOT>/docs/superpowers/prototypes/` (directory created on first use).

**Skill file contains:**
- Trigger pattern and input parsing instructions
- Initialization step (template fetch, hard-stop on failure)
- Parallel research subagent dispatch instructions with output schemas (as defined above)
- Synthesis agent instructions (distill with conflict priority rule → Q&A with 5-question budget → one-round follow-up → compile briefing)
- Writer agent instructions (template-driven section order, uncertainty handling, Notion output)
- All required constants

---

## Integration Tier Reference

| Tier     | Features / Coverage                         | Scale / Notes        |
|----------|---------------------------------------------|----------------------|
| Bronze   | Basic metadata collection, SQL queries      | ~1,000 tables        |
| Silver   | Metadata, volume, freshness                 | ~5,000 tables        |
| Gold     | Query logs, lineage                         | ~100,000 tables      |
| Platinum | Performance observability, full-scale       | Any scale            |

---

## References

- [Integration SDD Template (Notion)](https://www.notion.so/montecarlodata/Integration-SDD-Template-2aa334399e65801c8b5fe48f1448b22d)
- [Monolith Integration Docs](https://github.com/monte-carlo-data/monolith-django/blob/2165ca89527af48c610ac149bb2d95f102bcd8eb/docs/integrations.md)
- [Data Collector Integration Docs](https://github.com/monte-carlo-data/data-collector/blob/273c96b55affa0a6969d5bb1ea811c55d81ca7fd/docs/integrations.md)
- Apollo agent integrations: `apollo-agent/apollo/integrations/`
