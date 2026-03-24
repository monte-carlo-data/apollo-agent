---
name: research-integration
description: Use when researching a new Monte Carlo integration from a named data source — covers connection/auth options, metadata extraction, normalization, architecture, onboarding, and rollout estimates for a new integration SDD
---

# Research Integration Skill

## Overview

Produces a complete, filled-out Integration SDD (Software Design Document) in Notion for a named data source. Combines web research, codebase pattern extraction, live connection prototyping, and a single human checkpoint.

**Invocation:**
```
/research-integration <source-name> [-- <optional notes or credentials>]
```

Examples:
```
/research-integration Firebolt
/research-integration "Oracle Analytics Cloud" -- customer uses OAuth only
/research-integration Teradata -- host=foo.teradata.com user=test password=xxx
```

**Rigid skill.** Do not skip phases, inline subagent work, or add extra Q&A rounds.
Rigid means: follow the exact flow below. No exceptions.

---

## Install

Before first use, edit the four values marked `<fill in>` in the Constants table below.
Everything else is already correct for all Monte Carlo engineers.

**Quick setup** (replace `yourname` with your system username):
```
NOTION_SDD_AUTHOR     → your name as it should appear in Notion (e.g. jsmith)
APOLLO_AGENT_ROOT     → /Users/yourname/monte-carlo/apollo-agent
DATA_COLLECTOR_ROOT   → /Users/yourname/monte-carlo/data-collector
MONOLITH_ROOT         → /Users/yourname/monte-carlo/monolith-django
FRONTEND_ROOT         → /Users/yourname/monte-carlo/frontend
SAAS_SERVERLESS_ROOT  → /Users/yourname/monte-carlo/saas-serverless
```

If any repo isn't checked out locally, leave that path as `<not checked out>` —
the Codebase Pattern Agent will skip it gracefully and note it in `unreachable_repos`.

## Constants

| Constant | Description | Value |
|----------|-------------|-------|
| `NOTION_SDD_TEMPLATE_URL` | URL of the SDD template page | `https://www.notion.so/montecarlodata/Integration-SDD-Template-2aa334399e65801c8b5fe48f1448b22d` |
| `NOTION_SDD_PARENT_PAGE_ID` | Notion page ID of the SDDs parent | `2b1334399e6580e7a5ead2e8bc959709` |
| `NOTION_SDD_AUTHOR` | Your name for the SDD Author field | `swaller` |
| `APOLLO_AGENT_ROOT` | Absolute path to apollo-agent repo | `/Users/swaller/monte-carlo/apollo-agent` |
| `DATA_COLLECTOR_ROOT` | Absolute path to data-collector repo | `/Users/swaller/monte-carlo/data-collector` |
| `MONOLITH_ROOT` | Absolute path to monolith-django repo | `/Users/swaller/monte-carlo/monolith-django` |
| `FRONTEND_ROOT` | Absolute path to frontend repo | `/Users/swaller/monte-carlo/frontend` |
| `SAAS_SERVERLESS_ROOT` | Absolute path to saas-serverless repo | `/Users/swaller/monte-carlo/saas-serverless` |

---

## Step 1: Initialization (before any subagents)

Fetch the Notion SDD template using `NOTION_SDD_TEMPLATE_URL`.

**If the fetch fails** (API error, auth error, page unavailable): halt immediately.
Tell the user: "Could not fetch SDD template. Check your Notion MCP connection and NOTION_SDD_TEMPLATE_URL constant before retrying."
Do not proceed without the template.

Store the fetched template's section list and table schemas — include it in the writer briefing at the end.

Also scan the template for **standing requirements** — imperative statements that apply to all integrations (e.g., lines containing "must", "required for all", "We should also plan on", "all new integrations"). Store these as `template_standing_requirements` and include them in the writer briefing. The writer agent must implement all of them regardless of whether they were surfaced by research agents.

**Credential collection:** After the template fetch, ask the user:

> "To run a live prototype connection test, please provide credentials for [Integration Name]
> (e.g. `host=foo user=bar password=xxx`), or type `skip` to proceed without a live test.
> If skipped, the prototype will write client code conceptually and mark live results [PROTOTYPE NOT TESTED]."

Credentials can also be passed inline at invocation: `/research-integration Firebolt -- host=foo user=bar`.
Store credentials for the Prototype Agent. If skipped, store `credentials: null`.

---

## Step 2: Dispatch parallel research subagents

After template fetch and credential collection, dispatch both research subagents simultaneously.
**REQUIRED SUB-SKILL:** Use superpowers:dispatching-parallel-agents.

Full subagent prompt templates are in `subagents.md` (same directory as this skill file).

- **Web Research Agent** — use the "Web Research Agent Prompt" template from subagents.md
- **Codebase Pattern Agent** — use the "Codebase Pattern Agent Prompt" template from subagents.md

Wait for both to return before proceeding to Step 2b.

---

## Step 2b: Dispatch Prototype Agent

After both research subagents return, dispatch the Prototype Agent sequentially — it needs their findings.
Use the "Prototype Agent Prompt" template from subagents.md.
Pass: web findings, codebase findings, credentials (or null), APOLLO_AGENT_ROOT, today's date.

Wait for the Prototype Agent to return before proceeding to Step 3.

---

## Step 3: Synthesis agent

You (the synthesis agent) now hold all research and prototype findings.
Own this context — do not dispatch another subagent to distill or summarize.

### Distill

Compress web, codebase, and prototype findings into a working brief. Conflict priority:
- **Web research governs** what the vendor supports (auth options, APIs, system views)
- **Codebase patterns govern** how Monte Carlo implements it (proxy client shape, CCP config, DC plugin layout)
- **Prototype results are ground truth** for what actually works — override vendor doc claims
- **Conflicts spanning both** (e.g., vendor says X works but prototype failed) → surface as Q&A question

### Notify user if codebase was unreachable

If `unreachable_repos` is non-empty, tell the user BEFORE asking questions:

> "⚠️ Could not access: [repo names]. Codebase pattern findings may be incomplete.
> You can fix the path constants and retry, or continue with [NEEDS REVIEW] markers."

Wait for their response before proceeding.

### Q&A checkpoint (max 5 questions)

Present a human-readable summary then ask targeted questions only for unknowns.

**Prioritize questions:**
1. Questions that would change the integration tier estimate
2. Questions affecting connection/auth (most implementation-critical)
3. Questions affecting data extraction coverage (especially for failed prototype queries)
4. Everything else → mark [NEEDS REVIEW] in the document instead

**Format:**

> **Research findings for [Integration Name]:**
>
> - **Drivers:** [summary]
> - **Auth:** [summary]
> - **Prototype:** [connection succeeded/failed/skipped] — [one-line metadata query summary]
> - **Metadata confirmed:** tables ✓/✗, columns ✓/✗, query logs ✓/✗, lineage ✓/✗, volume ✓/✗, freshness ✓/✗
> - **Estimated tier:** [Bronze/Silver/Gold/Platinum] — [rationale based on confirmed metadata]
> - **Closest analog:** [integration name] ([similarity reason])
>
> **Questions before I write the SDD (max 5):**
> 1. [question]

This is the **single checkpoint**. After the user responds, proceed to Step 4.
Do NOT ask follow-up questions. Do NOT loop back.

### Follow-up research (one round max, if needed)

If the user's answer requires additional investigation, dispatch ONE targeted follow-up
subagent with: both agent findings + prototype results + user's answer verbatim + specific question.
After it returns, proceed to Step 4 regardless. Remaining unknowns → [NEEDS REVIEW].

### Compile writer briefing

```json
{
  "integration_name": "...",
  "user_notes": "...",
  "estimated_tier": "Bronze|Silver|Gold|Platinum",
  "tier_rationale": "...",
  "web_findings": {},
  "codebase_findings": {},
  "prototype_results": {},
  "human_answers": [{"question": "...", "answer": "..."}],
  "follow_up_findings": null,
  "template_structure": {},
  "template_standing_requirements": ["..."],
  "analog_integrations": [],
  "unresolved_gaps": []
}
```

---

## Step 4: SDD writer agent

Dispatch a general-purpose subagent with the complete writer briefing.

**Writer agent prompt template:**

---

You are writing a complete Integration SDD (Software Design Document) for **{{INTEGRATION_NAME}}**.

You have a complete research briefing:

```
{{WRITER_BRIEFING_JSON}}
```

Write the SDD using the template structure in `briefing.template_structure`.
The Notion template is authoritative — follow its section list and order exactly.

**For each section:**
- Write concrete, specific content based on the briefing. No placeholder text.
- Fill all tables with real data from the findings.
- **Follow template table schemas exactly — do not add or remove columns.**
  Use the column names and order from `briefing.template_structure` verbatim.
- Use `prototype_results` as ground truth: confirmed queries get real example SQL
  and sample output; failed/skipped queries are flagged accurately.
- If the briefing has an `unresolved_gaps` entry that affects a section,
  mark that content: `[NEEDS REVIEW: <reason from gap>]`
- Cross-reference prior sections naturally (e.g., normalization should reference
  the exact extraction sources identified in the Data Extraction section).
- In the Architecture section, reference the prototype file path as a starting point.
- Implement all `briefing.template_standing_requirements` — these are requirements
  that apply to every integration and must appear in the document regardless of
  whether they were surfaced by research agents.

**Effort and tier estimation (Rollout section):**
- Use `briefing.estimated_tier` as the starting point
- Adjust based on what you've written in prior sections
- State estimates as ranges (e.g., "3–5 days") with explicit assumptions
- Do not use point estimates

**Notion output:**
1. Create a new Notion page as a child of parent page ID: `{{NOTION_SDD_PARENT_PAGE_ID}}`
2. Title: "[Integration Name] Integration SDD"
3. Fill sections in template order
4. Set properties: Author = `{{NOTION_SDD_AUTHOR}}`, Status = "Draft"
5. Leave PRD and Feasibility Document links blank
6. Return the direct Notion page URL as your final message

---

After dispatching the writer agent, wait for it to return the Notion URL.
Return the URL to the user as your final output.
