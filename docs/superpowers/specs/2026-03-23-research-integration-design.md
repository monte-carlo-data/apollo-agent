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

**Output:** A Notion SDD page (status: Draft) created under the existing SDDs parent, with the URL returned to the user on completion.

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
 │  • Dispatches 3 parallel research subagents         │
 └─────────────┬───────────────────┬───────────────────┘
               │                   │                   │
               ▼                   ▼                   ▼
     ┌──────────────┐   ┌──────────────────┐  ┌─────────────────┐
     │  Web Research │   │ Codebase Pattern │  │  Notion Fetcher │
     │  Agent        │   │ Agent            │  │  Agent          │
     └──────┬───────┘   └────────┬─────────┘  └──────┬──────────┘
            └────────────────────┴────────────────────┘
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
- **One human checkpoint.** After research, before writing. Targeted questions only — things the agent genuinely could not determine.
- **One writer.** A single SDD writer agent (not parallel section writers) ensures cross-section coherence. Decisions made in "Connection & Authentication" carry naturally into "Normalization" and "Rollout Estimates."
- **Structured subagent output.** All subagents return structured findings (not prose) so the synthesis agent can distill them compactly and pass clean briefings to follow-up agents if needed.
- **Rigid skill.** The parallel dispatch pattern and single human checkpoint are load-bearing. The skill must be followed exactly.

---

## Phase 1: Parallel Research Subagents

Three subagents are dispatched simultaneously when the skill is invoked.

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

**Returns:** Structured findings object with one section per SDD area (connection, auth, data extraction, known limitations).

### Agent 2: Codebase Pattern Agent

**Input:** Integration name. Searches the following canonical paths:
- `apollo-agent/apollo/integrations/` — proxy client structure, CCP config, credential shape, transport handling
- `data-collector/` — DC plugin structure, metadata extraction implementation for analogous sources, credential model shape
- `monolith/` — connection model fields, warehouse model, GraphQL mutations for onboarding

**Identifies:** The 1–2 closest analogous existing integrations and extracts:
- Proxy client structure and credential shape
- CCP config pattern (if one exists for the analog)
- DC plugin layout and metadata extraction approach
- Monolith connection/warehouse model fields

**Returns:** Named analog integrations, reusable patterns, and structural gaps where the new integration will differ.

### Agent 3: Notion Template Agent

**Input:** SDD template URL (`https://www.notion.so/montecarlodata/Integration-SDD-Template-2aa334399e65801c8b5fe48f1448b22d`)

**Fetches:** The template page and returns a structured list of sections with their descriptions, embedded table schemas, and section prompts.

**Returns:** Clean template scaffold for the writer agent.

---

## Phase 2: Synthesis Agent

Receives all three sets of structured findings. Owns the complete context from this point forward.

### Step 1: Distill

Compresses research into a working brief. Resolves conflicts between web findings and codebase patterns (e.g., if two driver options exist but Monte Carlo already uses one for a similar integration, notes the preferred path).

### Step 2: Surface findings + targeted Q&A

Presents a human-readable summary to the user:
- What it found: connection options, auth methods, available metadata sources, nearest analog integration
- Estimated integration tier (Bronze/Silver/Gold/Platinum) based on available metadata APIs
- A short list of targeted questions — only things it could not determine from research

Example questions:
- "The vendor docs show both OAuth and API key auth — does the customer require a specific one?"
- "No system view for query logs was found in public docs — do you have access to internal docs or a test environment?"
- "The onboarding UI flow isn't clear from docs — should this follow the same pattern as Databricks or Snowflake?"

This is the **single human checkpoint**.

### Step 3: Handle follow-up research

If human feedback requires additional investigation, the synthesis agent dispatches a targeted follow-up subagent with a complete briefing: original findings + human feedback + specific question. The synthesis agent waits for results before continuing.

### Step 4: Compile writer briefing

Assembles a single structured document containing:
- All research findings (web + codebase patterns)
- Human-provided context and decisions
- Notion template structure (from Agent 3)
- Named analog integrations to reference for code structure

This briefing is the sole input to the writer agent.

---

## Phase 3: SDD Writer Agent

Receives the complete briefing. Produces the finished SDD in Notion.

### Writing approach

Works through SDD sections in order, using each prior section as context for the next. This is how cross-section coherence is maintained.

Sections covered (in order):
1. Context
2. Architecture Overview (component table + description)
3. Connection & Authentication (protocols, auth, SSL, timeouts, Private Link, self-hosted credentials)
4. Data Extraction (extraction details table: tables, columns, query logs, lineage, volume, freshness, metric monitors, raw SQL)
5. Normalization (mapping to Monte Carlo internal models)
6. Scalability
7. Risks and Assumptions
8. Design Rationale & Decision Points (table)
9. Frontend / Onboarding (connection arguments, conditional params, UI/UX, validations)
10. API, UI, CLI notes
11. Rollout Plan and Estimates (testing, tier classification, effort ranges)
12. References

For each section:
- Fills content with concrete, specific findings — no placeholder text
- Populates structured tables with real data
- Marks uncertain content with `[NEEDS REVIEW: <reason>]` rather than guessing

### Effort and tier estimation

Derives tier classification from:
- Which metadata types are accessible (tables/columns/query logs/lineage)
- How accessible those sources are (native API vs. fragile scraping vs. unavailable)
- Auth and credential complexity relative to existing integrations
- Whether a new proxy client type is needed or an existing one can be extended

Estimates expressed as ranges with explicit assumptions, not point estimates.

### Notion output

- Creates a new Notion page as a child of the existing SDDs parent page
- Uses the SDD template structure
- Sets: Author = invoking user, Status = "Draft", PRD/Feasibility links left blank
- Returns the direct Notion URL as the final output

---

## Skill File

**Location:** `~/.claude/skills/research-integration.md`

**Skill type:** Rigid — the parallel dispatch pattern and single human checkpoint are load-bearing and must not be adapted away.

**Skill file contains:**
- Trigger pattern and input parsing instructions
- Parallel subagent dispatch instructions with explicit output schemas
- Synthesis agent instructions (distill → Q&A → compile briefing)
- Writer agent instructions (section order, uncertainty handling, Notion output)
- Notion SDD template URL as a reference constant
- Canonical codebase paths to search

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
