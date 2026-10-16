---
name: fix-repo-vulns
description: "Remediate the open security findings in ONE checked-out repo and open one DRAFT pull request per finding. Invoked by the rybot vuln-fix autobuilder (GHA workflow rybot-fix-vulns.yml), once per repo. Enumerates findings itself: Aikido via Aikido's official MCP (@aikidosec/mcp), Dependabot via the GitHub API. Dependency bumps are mechanical; code-level SAST fixes are attempted but flagged for mandatory human verification. Never merges, never fabricates a fix."
---

# fix-repo-vulns

You remediate the open security findings in **one** repo (already checked out
for you) and open **one DRAFT pull request per finding**. **Do the work — find
the findings, fix them, open the PRs.**

## Inputs
- `target_repo` (env / prompt): `monte-carlo-data/<repo>`, checked out at
  `$TARGET_DIR` (your working directory).
- `sources`: comma list, any of `dependabot`, `aikido` (default both).
- `gh` is pre-authed with the mc-rybot App token (branch push + PR).

## Step 1 — enumerate the open findings (you do this, not rybot)

**Dependabot** (if `dependabot` in sources):
`gh api repos/<target_repo>/dependabot/alerts -f state=open --paginate`.
Each alert → a dependency finding: package, ecosystem, manifest_path,
vulnerable range, first-patched version, GHSA, severity, html_url.

**Aikido** (if `aikido` in sources): use **Aikido's MCP** (`@aikidosec/mcp`,
already configured for you as the `aikido` MCP server). Call its finding-listing
tool filtered to `repo_name = <repo short name>` and open status, across the
fixable issue types (`sast`, `open_source`, `leaked_secret`, `iac`, `eol`).
- If the MCP's first call returns Aikido **sign-in links** instead of findings,
  its credentials aren't configured in this run — record that Aikido was
  skipped (creds missing) and continue with Dependabot only. Do not try to log
  in interactively.
- Classify each Aikido finding: `open_source`/`eol` → dependency (a bump —
  including a transitive override or a base-image/Dockerfile bump when the
  vulnerable package lives in the built image rather than a source manifest);
  `sast`/`iac`/`leaked_secret` → code (needs a code/config change). A container /
  OS / base-image / stdlib finding is dependency-class (mechanical, PR A) — fix
  it with a Dockerfile base/toolchain bump or OS-package pin, NOT a punt.

## Step 2 — dedupe against existing PRs

There are at most TWO batch branches per repo: `rybot/vulnfix-deps` and
`rybot/vulnfix-code`. Before starting each batch, check for an already-open
rybot PR: `gh pr list --repo <target_repo> --state open --search "rybot vulnfix"`.
If the deps batch PR is already open, skip the deps batch this run; same for
code. (v1: skip if open. Updating an existing batch PR in place is a future
enhancement.) Cap at 25 findings total per run; if more, take the highest
severities first and report `left_over`.

## Step 2.5 — triage each finding with Tank (real vs false-positive)

Before fixing, run each finding through the mc-security **Tank** judge so we do
not "fix" or document false positives. A wrapper is provided at
`.claude/skills/fix-repo-vulns/tank_triage.py` (needs `TANK_SRC` +
`ANTHROPIC_API_KEY`, both set by the workflow). Per finding:

```
echo '{"source":"aikido","finding_type":"<sca|sast|iac|leaked_secret>","title":"<title>","context":"<affected pkg/file, fixed version, reachability>"}' \
  | python3 .claude/skills/fix-repo-vulns/tank_triage.py
```
It prints `{"disposition","reachability","rationale","parse_ok"}`. Route:
- **REAL_VULN** → fix it (Step 3); put Tank's reachability + rationale in the PR line.
- **FALSE_POSITIVE** (only when `parse_ok` is true) → do NOT fix. List it under a
  **"Suppression candidates (human-gated)"** section in the code PR body with
  Tank's rationale. NEVER suppress it or close a ticket yourself (see guardrails).
- **EOL_SNOOZE / TRANSITIVE_ONLY / NOT_APPLICABLE** → deprioritize (routine bump or
  a documented note, not an urgent fix).
- **PARSE_ERROR / `parse_ok:false`** (import or model error) → treat as NO
  disposition: fall back to fixing/documenting as usual. Never coerce to FP.

This is Tank's judgment ONLY — side-effect-free and eval-gated. rybot proposes
suppressions; it never executes them.

## Step 3 — fix findings, batched by type into at most TWO DRAFT PRs

**Why batched, not one-per-finding:** each merged PR redeploys the service.
On a production repo, 20 per-finding PRs would be 20 restarts. So a run opens
**at most two** DRAFT PRs: one for all dependency bumps (mechanical, merge
fast, one restart) and one for all code/SAST fixes (each needs review). Split
the findings by `kind`: `dependency` → PR A, `code` → PR B. If a batch has zero
findings, open no PR for it.

**What counts as fixable — do NOT punt these.** "Needs a lockfile", "no override
convention", "transitive, not directly pinned", or "it's a base-image / stdlib
CVE" are NOT reasons to skip — those describe roughly half of real remediations,
and many are literally "bump a pin and let the image rebuild." If you can express
the fix as a committed change that closes the finding on merge, it is IN SCOPE.
Concretely:

- **Direct dependency** in a manifest → bump to the smallest fixed version.
- **Transitive dependency** (not directly pinned) → FORCE it with the ecosystem's
  override mechanism: npm/pnpm `overrides`, yarn `resolutions`; Python: a pinned
  floor in `requirements.txt`/`pyproject.toml` or a `constraints.txt`; Go
  `replace` / `go get`. This is exactly what the deps PRs already do for
  fast-uri/sharp/postcss — apply it uniformly, don't special-case "no override
  convention" into a punt.
- **No committed lockfile** → generate one (`npm i --package-lock-only`,
  `pip-compile` / `uv lock`) or add the pin/override to the manifest. A missing
  lockfile is something to fix, not a reason to skip.
- **OS / base-image / stdlib / toolchain CVE** (finding is in the BUILT IMAGE, not
  a source manifest) → bump the pinned base-image tag or language/toolchain
  version in the `Dockerfile` (`FROM …:<patched>`), or pin the OS package
  (`apt-get install pkg=<fixed>` / `apk add pkg=<fixed>`). **The PR merging
  triggers the image rebuild — producing that PR IS the fix.** Prefer the repo's
  approved base-image source if it uses one; never pull a base image from an
  unapproved registry. If you can't identify a patched tag, say which image and
  current-vs-needed — don't write "needs a manual rebuild."
- **Genuinely unfixable only** (no patched version exists anywhere, or the fix
  needs a migration/judgment you can't safely make) → "Not fixed / manual" with
  the SPECIFIC reason, never a category punt.

Base-image / Dockerfile / lockfile-introduction changes are mechanical and ride
**PR A** — on merge they rebuild the image, same as a dependency bump.

### PR A — dependency + image bumps (ONE PR for all mechanical fixes)
- Branch `rybot/vulnfix-deps` off the default branch.
- Apply **every** dependency bump into the manifest(s) and refresh the
  lockfile **once** (npm/yarn/pnpm, pip/poetry/uv, go mod, etc.). Smallest bump
  that reaches each fixed version. Use overrides/resolutions/constraints for
  transitives, and generate a lockfile if the project lacks one (see above). Also
  apply any base-image / Dockerfile / OS-package bumps here — they're mechanical
  and rebuild the image on merge. One commit is fine; per-package commits are
  fine too.
- A dep or image finding with **no patched version anywhere**: don't fake it —
  list it under "Not fixed / manual" in the body with the specific reason (e.g.
  "no fixed release published yet"). It must not block the others.
- **Verify + attest — this is what lets the change merge UNATTENDED (do not
  skip).** The review gate fails closed on *missing evidence*, not on the bump
  itself — an unbuilt, unverified lockfile reads as a supply-chain risk and gets
  held. So PRODUCE the evidence, from real tools, and paste their RAW output
  into the PR (model prose is not evidence). For a deps/image PR, run and capture:
  - **Install/build for real** (NOT `--package-lock-only` alone): `npm ci
    --ignore-scripts` (credential-free) + the project's build/typecheck; Python:
    resolve the lock (`uv lock` / `pip install --dry-run`) + import/build smoke.
    `--ignore-scripts` first so a hostile postinstall can't run during verify.
  - **Prove every NEW or OVERRIDDEN package is real**, for each one:
    `npm view <pkg>@<ver> name version dist.integrity maintainers` (Python:
    `pip index versions <pkg>` / PyPI JSON). Capture name, version, publisher/
    maintainers, and integrity. This is the guard that both (a) kills the
    false-positive typosquat flag on legitimate packages and (b) actually stops a
    real typosquat. **If a package can't be verified (no registry entry, unknown
    publisher, integrity mismatch) → do NOT pin it; treat it as a genuine
    supply-chain stop and flag it.**
  - **Lockfile provenance**: `npx lockfile-lint --path <lockfile> --validate-https
    --validate-integrity --allowed-hosts npm` (assert every resolved URL is the
    real registry). Python: assert all sources are PyPI.
  - **Post-fix scan clean**: `npm audit` / `pip-audit` → capture "0 vulnerabilities".
  - **Corroborate advisories**: confirm each GHSA/CVE/Aikido ID resolves to the
    package+range you bumped; note any that don't.
  Never claim a check you didn't run. If a build/verify step genuinely can't run
  in CI, say precisely which and why — don't paper over it.
- Open the DRAFT PR (`gh pr create --draft`). Title:
  `rybot: batch dependency security bumps (<N> findings)`. Body: a table of each
  finding (package, X->Y, severity, GHSA, url), a "Not fixed / manual" section,
  and a **`## Provenance & verification`** section containing the raw tool output
  above (build result, per-package registry+integrity proof, lockfile-lint,
  audit=0, advisory corroboration). That section is the evidence the automated
  review keys on — a PR that carries it can clear review and auto-merge without a
  human; a PR that omits it will (correctly) be held.

### PR B — code/SAST fixes (ONE PR, one commit per finding)
- Branch `rybot/vulnfix-code` off the default branch.
- For each code finding, make the **minimal** targeted fix as its **own commit**
  (so each is individually reviewable/revertable), commit message naming the
  finding. Do not refactor; touch only what the finding needs.
- **Never fabricate.** If a fix isn't clearly correct (ambiguous SAST, no known
  patch, a change needing a migration you can't confidently make), make NO code
  change for it — record it under "Needs manual remediation" in the body with
  your analysis + `TODO(human):`.
- Verify best-effort once at the end. Note the result.
- Open the DRAFT PR. Title: `rybot: code/SAST security fixes (<N>, human-verify)`.
  Body, in this order:
  ```
  ## ⚠️ Review required
  AUTOGENERATED security-code changes. A human MUST review and verify EACH
  commit fixes its finding without regressions before merge. rybot never merges.
  ## Fixes (one commit each)
  - <sha-ish/order> <severity> <file>:<line> <rule> — <what changed>
  ## Needs manual remediation (no code change made)
  - <severity> <file> <rule> — <why not auto-fixed> · TODO(human)
  ## Verification
  <what you ran + result, or "not run — toolchain unavailable in CI">
  Autobuilt by rybot. DRAFT.
  ```

## Hard guardrails
- DRAFT PRs only. Never merge / mark ready.
- **At most TWO PRs per repo per run** — one deps, one code/SAST. NEVER
  one-per-finding (restart cost on prod services).
- Never claim a code fix is verified/safe.
- Never invent a fixed version or a patch you're unsure of — document it instead.
- **Never suppress a finding or close a ticket.** Tank triage is judgment-only;
  a `FALSE_POSITIVE` verdict means *propose* suppression in the PR body for a
  human, never execute it (Aikido suppression is group-scoped and can silence
  unrelated repos).

## Final output
Print one JSON line the caller can parse:
`{"status":"done","repo":"<target_repo>","deps_pr":"<url|empty>","code_pr":"<url|empty>","deps_fixed":<n>,"code_fixed":<n>,"documented":<n>,"fp_candidates":<n>,"aikido":"ok|creds-missing","left_over":<n>}`.
