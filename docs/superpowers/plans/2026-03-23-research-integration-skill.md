# Research Integration Skill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a `/research-integration` Claude skill that autonomously researches a named data source, runs a single human checkpoint, and publishes a complete filled-out SDD to Notion.

**Architecture:** Skill file triggers initialization (Notion template fetch), then dispatches two parallel research subagents (web + codebase), feeds results into a synthesis agent that runs a single human Q&A checkpoint, then dispatches a dedicated SDD writer agent to produce the Notion page. The skill is a markdown prompt file following the superpowers skill format; agent prompts are split into a supporting file to stay within token budget.

**Tech Stack:** Claude skill (markdown prompt), Notion MCP (`mcp__claude_ai_Notion__*`), superpowers:dispatching-parallel-agents for parallel subagent dispatch, superpowers:writing-skills for skill authoring discipline.

**Spec:** `docs/superpowers/specs/2026-03-23-research-integration-design.md`

---

## File Structure

```
~/.claude/skills/research-integration/
  SKILL.md          # Main skill: frontmatter, flow, constants, synthesis + writer instructions
  subagents.md      # Heavy reference: full web research + codebase agent prompt templates
```

**Why split?** The two research agent prompts each contain full instructions + output schemas (~80–100 lines each). Keeping them in a separate file respects the superpowers skill token budget (<500 words for SKILL.md) while keeping the full detail available for subagent dispatch. The synthesis and writer agent instructions are shorter (<60 lines each) and stay inline in SKILL.md.

---

## Task 1: Directory structure and RED baseline test

**Files:**
- Create: `~/.claude/skills/research-integration/` (directory)
- Create: `~/.claude/skills/research-integration/baseline-test.md`

The writing-skills skill requires running a baseline scenario WITHOUT the skill first, to document what Claude does naturally. This is the RED phase.

- [ ] **Step 1: Create the skill directory**

```bash
mkdir -p ~/.claude/skills/research-integration
```

- [ ] **Step 2: Write the baseline test scenario**

Create `~/.claude/skills/research-integration/baseline-test.md` with this content:

```markdown
# Baseline Test Scenario: research-integration skill

## Pressure scenario (run WITHOUT skill loaded)

Dispatch a subagent with this prompt:

"You are helping research a new data source integration for Monte Carlo.
The integration is for: Firebolt (a cloud analytics database).

Research what it would take to integrate Firebolt into Monte Carlo and
produce a filled-out Software Design Document covering:
- Connection and authentication options
- What metadata (tables, columns, query logs, lineage) can be extracted and from where
- How this maps to Monte Carlo's internal models
- Architecture overview
- Onboarding/frontend flow
- Rollout estimates and tier classification

Use this SDD template as your structure:
https://www.notion.so/montecarlodata/Integration-SDD-Template-2aa334399e65801c8b5fe48f1448b22d

You have access to web search and the codebase at:
- /Users/swaller/monte-carlo/apollo-agent/apollo/integrations/
- /Users/swaller/monte-carlo/data-collector/
- /Users/swaller/monte-carlo/monolith-django/

Produce the best SDD you can."

## What to document after the baseline run

- Did it fetch the Notion template first?
- Did it search the codebase for analogous integrations?
- Did it run web research?
- Did it ask the human any questions before writing?
- How complete and specific was the output?
- What was missing or vague?
```

- [ ] **Step 3: Run the baseline test**

Dispatch a subagent using the scenario above. Observe its behavior carefully.

- [ ] **Step 4: Document baseline failures**

Add a "Baseline Results" section to `baseline-test.md`. Record:
- Which phases (web research, codebase search, human Q&A) it skipped or did ad-hoc
- Quality gaps in the output (vague sections, missing tables, placeholder text)
- Any structure it imposed on its own

This is the evidence the skill needs to address.

---

## Task 2: Write SKILL.md — frontmatter, overview, constants

**Files:**
- Create: `~/.claude/skills/research-integration/SKILL.md`

- [ ] **Step 1: Write SKILL.md with frontmatter and overview**

Create `~/.claude/skills/research-integration/SKILL.md`:

```markdown
---
name: research-integration
description: Use when researching a new Monte Carlo integration from a named data source — covers connection/auth options, metadata extraction, normalization, architecture, onboarding, and rollout estimates for a new integration SDD
---

# Research Integration Skill

## Overview

Produces a complete, filled-out Integration SDD (Software Design Document) in Notion for a named data source. Combines web research, codebase pattern extraction, and a single human checkpoint.

**Invocation:**
```
/research-integration <source-name> [-- <optional notes>]
```

Examples:
```
/research-integration Firebolt
/research-integration "Oracle Analytics Cloud" -- customer uses OAuth only
/research-integration Teradata -- self-hosted, private link required
```

**Rigid skill.** Do not skip phases, inline subagent work, or add extra Q&A rounds.
Rigid means: follow the exact flow below. No exceptions.

---

## Constants (fill in at install time)

Edit these values in this skill file before first use:

| Constant | Description | Example |
|----------|-------------|---------|
| `NOTION_SDD_TEMPLATE_URL` | URL of the SDD template page | `https://www.notion.so/montecarlodata/Integration-SDD-Template-2aa334399e65801c8b5fe48f1448b22d` |
| `NOTION_SDD_PARENT_PAGE_ID` | Notion page ID of the SDDs parent | `abc123def456` |
| `NOTION_SDD_AUTHOR` | Your name for the SDD Author field | `swaller` |
| `APOLLO_AGENT_ROOT` | Absolute path to apollo-agent repo | `/Users/swaller/monte-carlo/apollo-agent` |
| `DATA_COLLECTOR_ROOT` | Absolute path to data-collector repo | `/Users/swaller/monte-carlo/data-collector` |
| `MONOLITH_ROOT` | Absolute path to monolith-django repo | `/Users/swaller/monte-carlo/monolith-django` |
```

- [ ] **Step 2: Verify file was created and frontmatter is valid**

```bash
head -5 ~/.claude/skills/research-integration/SKILL.md
```

Expected: frontmatter with `name: research-integration` and description starting with "Use when".

---

## Task 3: Write SKILL.md — initialization + flow diagram

**Files:**
- Modify: `~/.claude/skills/research-integration/SKILL.md`

- [ ] **Step 1: Append initialization section to SKILL.md**

```markdown
---

## Step 1: Initialization (before any subagents)

Fetch the Notion SDD template using `NOTION_SDD_TEMPLATE_URL`.

**If the fetch fails** (API error, auth error, page unavailable): halt immediately.
Tell the user: "Could not fetch SDD template. Check your Notion MCP connection and NOTION_SDD_TEMPLATE_URL constant before retrying."
Do not proceed without the template.

Store the template's section list and table schemas — you will include it in the writer briefing.

---

## Step 2: Dispatch parallel research subagents

After template fetch succeeds, dispatch both research subagents simultaneously.
**REQUIRED SUB-SKILL:** Use superpowers:dispatching-parallel-agents.

Full subagent prompt templates are in `subagents.md` (same directory as this skill file).

- **Web Research Agent** — use the "Web Research Agent Prompt" template from subagents.md
- **Codebase Pattern Agent** — use the "Codebase Pattern Agent Prompt" template from subagents.md

Wait for both to return before proceeding.
```

- [ ] **Step 2: Verify the flow section reads cleanly**

Read back the file and confirm initialization and Step 2 are present.

---

## Task 4: Write subagents.md — research agent prompt templates

**Files:**
- Create: `~/.claude/skills/research-integration/subagents.md`

These are the full prompt templates passed to each research subagent. The synthesis agent populates `{{INTEGRATION_NAME}}`, `{{USER_NOTES}}`, and path constants before dispatching.

- [ ] **Step 1: Create subagents.md with the web research agent prompt**

```markdown
# Research Subagent Prompt Templates

---

## Web Research Agent Prompt

Use this as the complete prompt when dispatching the Web Research Agent.
Replace `{{INTEGRATION_NAME}}` and `{{USER_NOTES}}` before dispatching.

---

You are researching the **{{INTEGRATION_NAME}}** data source for a Monte Carlo integration.
{{USER_NOTES_LINE}}
(Replace {{USER_NOTES_LINE}} with "Additional context from the engineer: <notes>" if user notes were provided, or omit the line entirely if not.)

Research the following and return structured findings. Be specific — cite source URLs and
exact API/view names where found. If you cannot find something, say so explicitly
rather than guessing.

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
   - Lineage (if the vendor exposes query lineage or dependency info)
   - Volume (row counts or approximate table sizes)
   - Freshness (last modified time for tables)

5. **Known limitations**: Rate limits, row count caps, pagination requirements,
   permission requirements, anything that would affect large-scale metadata collection.

6. **Private Link support**: Does the vendor support AWS PrivateLink? Azure Private Link?
   Cite the vendor docs page if yes.

7. **Existing Monte Carlo docs**: Search for any existing Monte Carlo documentation,
   blog posts, or customer-facing references to {{INTEGRATION_NAME}}.

**Return your findings as a JSON object with this exact structure:**

```json
{
  "drivers": [
    {"name": "...", "type": "python|jdbc|rest", "package": "...", "notes": "..."}
  ],
  "auth_methods": [
    {"method": "...", "notes": "...", "preferred": true|false}
  ],
  "ssl_requirements": "...",
  "metadata_sources": {
    "tables": {"source": "...", "example_query": "...", "notes": "..."},
    "columns": {"source": "...", "example_query": "...", "notes": "..."},
    "query_logs": {"source": "...", "example_query": "...", "notes": "..."},
    "lineage": {"source": "...", "notes": "...", "available": true|false},
    "volume": {"source": "...", "example_query": "...", "notes": "..."},
    "freshness": {"source": "...", "example_query": "...", "notes": "..."}
  },
  "known_limitations": ["..."],
  "private_link_support": {"aws": true|false, "azure": true|false, "notes": "..."},
  "monte_carlo_existing_docs": "url or null",
  "sources_consulted": ["url1", "url2"]
}
```
```

- [ ] **Step 2: Append the codebase pattern agent prompt to subagents.md**

```markdown
---

## Codebase Pattern Agent Prompt

Use this as the complete prompt when dispatching the Codebase Pattern Agent.
Replace `{{INTEGRATION_NAME}}` and all path constants before dispatching.

---

You are finding the best analogous existing integrations in the Monte Carlo codebase
for a new **{{INTEGRATION_NAME}}** integration.

Search these directories:
- Apollo agent integrations: `{{APOLLO_AGENT_ROOT}}/apollo/integrations/`
- Data collector: `{{DATA_COLLECTOR_ROOT}}/`
- Monolith: `{{MONOLITH_ROOT}}/`

**Your goal:** Find the 1–2 existing integrations most similar to {{INTEGRATION_NAME}}
(consider: same protocol type, similar auth, similar metadata extraction approach).

For each analog integration you identify, extract:

1. **Proxy client** (apollo-agent):
   - File path
   - Class name and `__init__` signature
   - CCP config file path (if one exists under `apollo/integrations/ccp/defaults/`)
   - Credential shape (what keys does connect_args contain?)

2. **DC plugin** (data-collector):
   - File path
   - How metadata is extracted (query-based? API calls? special driver?)
   - Credential model class and fields

3. **Monolith models**:
   - Connection model class and file path
   - Warehouse model class (if applicable)
   - Key fields on these models

**If a configured path does not exist on this machine**, note it in `unreachable_repos`
and continue with whatever repos are accessible.

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
      "notable_patterns": ["..."]
    }
  ],
  "reusable_patterns": ["..."],
  "structural_gaps": ["..."],
  "unreachable_repos": []
}
```
```

- [ ] **Step 3: Verify subagents.md was created and both prompts are present**

```bash
grep -c "Agent Prompt" ~/.claude/skills/research-integration/subagents.md
```

Expected: `2`

---

## Task 5: Write SKILL.md — synthesis agent instructions

**Files:**
- Modify: `~/.claude/skills/research-integration/SKILL.md`

- [ ] **Step 1: Append synthesis agent section to SKILL.md**

```markdown
---

## Step 3: Synthesis agent

You (the synthesis agent) now hold all research findings. Own this context — do not
dispatch another subagent just to distill or summarize.

### Distill

Compress both agent findings into a working brief. Use this conflict priority rule:
- **Web research governs** what the vendor supports (auth, APIs, system views)
- **Codebase patterns govern** how Monte Carlo implements it (proxy client shape, CCP config, DC plugin layout)
- **Conflicts that span both** (e.g., a vendor auth type no analog uses) → surface as a question in the next step

### Notify user if codebase was unreachable

If `unreachable_repos` is non-empty in the codebase findings, tell the user BEFORE asking questions:

> "⚠️ Could not access: [repo names]. Codebase pattern findings may be incomplete.
> You can fix the path constants and retry, or continue and I'll mark affected sections [NEEDS REVIEW]."

Wait for their response before proceeding.

### Q&A checkpoint (max 5 questions)

Present a human-readable summary then ask targeted questions ONLY for things you could not determine.

**Prioritize questions:**
1. Questions that would change the integration tier estimate
2. Questions affecting connection/auth (most implementation-critical)
3. Questions affecting data extraction coverage
4. Everything else → mark [NEEDS REVIEW] in the document instead

**Format:**

> **Research findings for [Integration Name]:**
>
> - **Drivers:** [summary]
> - **Auth:** [summary]
> - **Metadata available:** tables ✓, columns ✓, query logs [yes/no/partial], lineage [yes/no], volume ✓, freshness [yes/no]
> - **Estimated tier:** [Bronze/Silver/Gold/Platinum] — [one-line rationale]
> - **Closest analog:** [integration name] ([similarity reason])
>
> **Questions before I write the SDD (max 5):**
> 1. [question]
> ...

This is the **single checkpoint**. After the user responds, proceed to Step 4.
Do NOT ask follow-up questions. Do NOT loop back.

### Follow-up research (one round max, if needed)

If the user's answer reveals something that requires additional research
(e.g., "the customer uses a vendor-specific auth plugin not in the public docs"),
dispatch ONE targeted follow-up subagent with:
- Both agent findings as context
- The user's answer verbatim
- The specific question to investigate

After it returns: proceed to Step 4 regardless. Any remaining unknowns become [NEEDS REVIEW].

### Compile writer briefing

Assemble the complete briefing for the writer agent:

```json
{
  "integration_name": "...",
  "user_notes": "...",
  "estimated_tier": "Bronze|Silver|Gold|Platinum",
  "tier_rationale": "...",
  "web_findings": { ...web agent output... },
  "codebase_findings": { ...codebase agent output... },
  "human_answers": [{"question": "...", "answer": "..."}],
  "follow_up_findings": { ...or null... },
  "template_structure": { ...sections from initialization... },
  "analog_integrations": [...],
  "unresolved_gaps": ["..."]
}
```
```

- [ ] **Step 2: Verify synthesis section is present**

```bash
grep -c "Q&A checkpoint" ~/.claude/skills/research-integration/SKILL.md
```

Expected: `1`

---

## Task 6: Write SKILL.md — writer agent instructions

**Files:**
- Modify: `~/.claude/skills/research-integration/SKILL.md`

- [ ] **Step 1: Append writer agent section to SKILL.md**

```markdown
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
- If the briefing has an `unresolved_gaps` entry that affects a section,
  mark that content: `[NEEDS REVIEW: <reason from gap>]`
- Cross-reference prior sections naturally (e.g., normalization should reference
  the exact extraction sources identified in the Data Extraction section).

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
```

- [ ] **Step 2: Verify writer section is complete**

```bash
wc -l ~/.claude/skills/research-integration/SKILL.md
```

Expected: 200–300 lines is normal given the full synthesis section above it.

---

## Task 7: GREEN test — run with skill loaded

Run the same scenario from Task 1 baseline, but now with the skill available. This is the GREEN phase.

- [ ] **Step 1: Invoke the skill with a simple integration**

In Claude Code, run:
```
/research-integration Firebolt
```

- [ ] **Step 2: Verify the flow was followed**

Check the agent's behavior against this checklist:
- [ ] It fetched the Notion SDD template before doing anything else
- [ ] It dispatched two research subagents (not one, not inline research)
- [ ] It presented a structured summary with an estimated tier before asking questions
- [ ] It asked 5 or fewer targeted questions (not a wall of questions)
- [ ] It wrote the full SDD to Notion and returned a URL
- [ ] The SDD has no obvious placeholder text

- [ ] **Step 3: Evaluate output quality**

Read the produced Notion page. Check:
- Does the Connection & Authentication section name real drivers/auth options?
- Does the Data Extraction table have real source views/API endpoints?
- Is the tier estimate reasonable given what's actually available?
- Does the Normalization section reference the actual extraction sources from the prior section?
- Are [NEEDS REVIEW] markers used only for genuinely uncertain items (not as a cop-out)?

Document any quality gaps in `baseline-test.md` under "GREEN test results".

---

## Task 8: REFACTOR — close loopholes

If the GREEN test revealed gaps in the skill instructions, fix them.

- [ ] **Step 1: Identify specific instruction failures**

For each quality gap documented in Task 7 Step 3:
- Is there a specific part of the skill instructions that was unclear or missing?
- What rationalization did the agent use that led to the gap?

- [ ] **Step 2: Update SKILL.md or subagents.md to address each gap**

Update the relevant section. Be specific — add explicit counters for any rationalizations you observed. Follow the superpowers:writing-skills pattern for closing loopholes.

- [ ] **Step 3: Re-run the test**

Run `/research-integration Firebolt` again and verify the gaps are closed.

- [ ] **Step 4: Test a second integration type (different category)**

Run the skill on a BI tool (not a SQL warehouse) to verify it generalizes:
```
/research-integration Tableau
```

Verify the codebase pattern agent finds the Tableau analog in apollo-agent and that the output reflects the different integration type.

- [ ] **Step 5: Commit the skill files**

```bash
# Skills live in ~/.claude/skills/ which is not a git repo
# Copy skill to the apollo-agent docs for version control and team sharing
cp -r ~/.claude/skills/research-integration \
  "$(git -C ~/.claude/skills 2>/dev/null rev-parse --show-toplevel || echo $(cd /Users/swaller/monte-carlo/apollo-agent && pwd))/docs/superpowers/skills/"
# Or more simply:
mkdir -p /Users/swaller/monte-carlo/apollo-agent/docs/superpowers/skills
cp -r ~/.claude/skills/research-integration \
  /Users/swaller/monte-carlo/apollo-agent/docs/superpowers/skills/

cd /Users/swaller/monte-carlo/apollo-agent
git add docs/superpowers/skills/research-integration/
git commit -m "feat: add research-integration skill

Adds /research-integration Claude skill that produces a filled-out
SDD in Notion from a named data source using parallel research
subagents, a single human checkpoint, and a dedicated writer agent.
"
```

---

## Notes for implementer

- **Skill type:** This is a "technique" skill (how-to guide), not a discipline-enforcing skill. The baseline test in Task 1 documents what happens without structure; the GREEN test verifies structure is followed.
- **Notion MCP:** The skill requires `mcp__claude_ai_Notion__*` tools. Verify your Notion MCP connection works before starting Task 7. Test with: `mcp__claude_ai_Notion__notion-fetch` on the SDD template URL.
- **Constants:** All 6 constants in SKILL.md must be filled in before the skill is usable. The plan tasks leave them as descriptive placeholders — fill them in during Task 2.
- **subagents.md template syntax:** The `{{VARIABLE}}` syntax in subagents.md is not a real templating engine — the synthesis agent fills these in by string substitution when constructing the actual subagent prompt. Make this clear in the skill if there's any ambiguity.
- **Skill directory vs. single file:** The spec describes the skill as a single file (`~/.claude/skills/research-integration.md`). The plan intentionally uses a directory with `SKILL.md` + `subagents.md` — this is a deliberate deviation to keep SKILL.md within the superpowers token budget while preserving the full agent prompts in a supporting file. The directory format is the superpowers-standard layout and is fully supported.
- **Saving plan location:** `docs/superpowers/plans/2026-03-23-research-integration-skill.md`
