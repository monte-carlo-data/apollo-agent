# research-integration skill

A Claude Code skill that researches a new Monte Carlo integration and produces a
filled-out Integration SDD in Notion. Runs parallel web research, codebase pattern
extraction, and a prototype connection test, then asks you up to 5 targeted questions
before writing the document.

**Invocation after install:**
```
/research-integration <source-name> [-- <optional notes or credentials>]
```

## Install

1. Copy this directory to your Claude skills folder:
   ```
   cp -r docs/superpowers/skills/research-integration ~/.claude/skills/research-integration
   ```

2. Edit `~/.claude/skills/research-integration/SKILL.md` — fill in the six constants
   under the **Constants** table (your name + your local repo paths). The Notion URL
   and parent page ID are already correct for all Monte Carlo engineers.

3. Make sure you have the Notion MCP server connected in Claude Code.

That's it. Run `/research-integration Firebolt` to verify the skill loads.

## Files

| File | Purpose |
|------|---------|
| `SKILL.md` | Main skill — orchestration flow, constants, all step instructions |
| `subagents.md` | Prompt templates for the web research, codebase pattern, and prototype subagents |
| `baseline-test.md` | RED baseline documentation — what an unguided agent produces without this skill |
