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

## Baseline Results

Documented from design process observations (unguided subagent behavior):

**Template fetch:** No — an unguided agent does not think to fetch the Notion SDD template
before writing. It writes freeform or guesses at a structure.

**Parallel research dispatch:** No — researches inline and serially, mixing web search
with writing rather than separating research and synthesis phases.

**Codebase pattern search:** Inconsistent — may search the codebase, but without knowing
which paths matter (integrations/, ccp/defaults/, DC plugins, monolith models) and
without extracting a structured analog comparison.

**Credential collection:** No — does not ask for credentials. Either skips connection
testing entirely or assumes it cannot test.

**Prototype client:** No — writes no client code. At best references existing code.

**Human Q&A before writing:** No — writes the document without a checkpoint.

**Output quality gaps:**
- Data Extraction table filled with assumed values rather than confirmed queries
- Tier estimate based on vendor docs only, not validated metadata availability
- No real SQL examples — placeholders not verified against actual vendor catalog views
- Architecture section vague, no reference to actual proxy client structure
- Onboarding section lists fields without specifying conditional/optional distinctions
- Effort estimates are point estimates without stated assumptions

**Key rationalizations an unguided agent uses:**
- "I'll research as I write each section" (no separation of research and synthesis)
- "I can infer the credential shape from similar integrations" (no validation)
- "The vendor docs say X is supported" (not confirmed by actual query test)
- "I'll use reasonable estimates" (not grounded in confirmed capability tier)
