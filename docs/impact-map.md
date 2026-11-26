# Impact map

`scripts/impact_map.py` tells CI what a packaging change can break: which images to build
and check, which platforms' test agents to run, which connection types to validate, and what a
person should check by hand. It is layer 2 of
[YET-3659](https://linear.app/montecarloai/issue/YET-3659): its output decides which image
targets the image contract tests build (layer 1) and which platforms and connection types the
standing test agents validate (layer 3).

It is stdlib only (Python 3.11+) and reads everything from git, so it runs in a bare CI Python
with no repo dependencies installed.

```bash
python scripts/impact_map.py --base origin/main --head HEAD                    # JSON
python scripts/impact_map.py --base origin/main --head HEAD --format markdown  # PR checklist
```

`--base` and `--head` are git refs. Everything is compared from `git merge-base base head`,
like a PR diff: both the list of changed files and the "before" contents of each requirements
file. So passing `origin/main` after `main` has moved on is safe: changes that landed on
`main` since the branch point aren't reported as reverted. The base ref must be fetched. Logs go to stderr. Exit code 0 means the analysis ran,
including "nothing changed"; anything else is an internal error (bad ref, unreadable manifest),
and callers should then build and check everything.

## Output

```json
{
  "schema_version": 1,
  "packaging_changed": true,
  "changed_files": ["requirements.txt"],
  "changed_packages": [{"name": "pyodbc", "from": "5.1.0", "to": "5.2.0", "requirements_files": ["requirements.txt"]}],
  "image_targets": ["generic", "cloudrun", "lambda", "azure"],
  "platforms": ["aws", "gcp", "azure"],
  "connection_types": ["azure-dedicated-sql-pool", "azure-sql-database", "microsoft-fabric", "sql-server"],
  "manual_checks": [{"area": "metadata_collection", "connection_type": "sql-server", "reason": "pyodbc 5.1.0 → 5.2.0: ..."}],
  "unmapped_packages": []
}
```

| Field | Meaning |
|---|---|
| `packaging_changed` | A file matching `packaging_paths` changed (`requirements*`, `Dockerfile`, `.dockerignore`, `resources/`). When false, every list is empty. |
| `changed_files` | The packaging files that changed. Code changes are not listed. |
| `changed_packages` | Pins that differ between the refs. `from`/`to` is `null` for a removed/added pin; a URL pin's version is its URL. |
| `image_targets` | ⊆ `generic`, `cloudrun`, `lambda`, `azure`. `aws_proxied` is `generic` with another `CMD`, so it is never listed: check its entry module in the `generic` image. |
| `platforms` | ⊆ `aws`, `gcp`, `azure`: the platforms of the selected targets (`lambda` → aws, `cloudrun` → gcp, `azure` → azure; `generic` has no standing test agent). |
| `connection_types` | REST API v2 connection types (`GET /connection-types`, the names the `apollo-agent-dev-smoke-test` skill takes), e.g. `sql-server`, `bigquery`, `databricks-sql-warehouse`. |
| `manual_checks` | Paths CI can't exercise. `area` ∈ `metadata_collection`, `query_logs`, `monitors`, `other`; `connection_type` is `null` for `other` checks that aren't about one connection type. |
| `unmapped_packages` | Packages it couldn't map. Non-empty means it failed open. |

`--format markdown` prints the manual checklist for a PR comment, and prints nothing when there
are no manual checks. The checklist never fails CI.

## How it works

1. **Changed files.** `git diff --name-only <merge-base> head`, filtered to `packaging_paths`.
2. **Fail-open paths.** A change to the `Dockerfile` (which holds the base images),
   `.dockerignore` or anything under `resources/` affects every image: all targets, all
   platforms, every connection type, plus an `other` manual check.
3. **Changed pins.** For each changed `requirements*.txt`, the pins at both refs are compared.
   The file maps to the image targets that install it (`[requirements_files]`); a file the
   manifest doesn't know fails open. `requirements-dev.txt` maps to no image, so a dev-only
   bump reports `packaging_changed: true` with no targets.
4. **`# via` walk.** pip-compile writes `# via <parent>` under each pin (`-r x.in` marks a
   top-level package; `-c requirements.txt` is a constraint and is ignored). From each changed
   pin the script walks every ancestor in that file, at both refs. A transitive bump (e.g.
   `cffi` ← `cryptography` ← `snowflake-connector-python`) therefore reaches the drivers that
   pull it in.
5. **Mapping.** For the changed package and each ancestor:
   - its `[packages]` entry adds `connection_types` and any `manual` checks;
   - **import scanning**: the `apollo/` files that import it at the head ref (read with one `git
     cat-file --batch` and parsed with `ast`, so multi-line imports count; `from a import b`
     counts as importing `a` and `a.b`) are looked up in `[modules]`, which
     adds that module's `connection_types` and turns its `manual` text into an `other` check.
     This catches packages apollo imports directly but only gets transitively, like `boto3`.
6. **Per-area checks.** When the changed package's own entry has `areas` (drivers do), each of
   its own connection types gets one manual check per area that type supports
   (`[connection_types]`). Connection validation only proves the client connects; metadata
   collection, query logs and monitors run other queries and result handling.
7. **Fail-open on unknowns.** A package whose top-level ancestor has no `[packages]` entry, a
   pin with no `# via` path to a known package, a `.in` change whose `.txt` wasn't recompiled,
   or a `.in` with no mapped `.txt` → every target, platform and connection type, with the
   package in `unmapped_packages` and an `other` check saying what to add.

A change to apollo code alone is not a packaging change. The script answers "what can this
dependency change break", not "what does this code change touch".

### Known limitations

- **Any `Dockerfile` or `resources/` change selects everything**: every image target, platform
  and connection type. That suits the image checks, but once the standing test agents validate
  connections, a one-line Dockerfile tweak means a full run, and vulnerability fixes touch the
  Dockerfile often. A stage-aware mapping (which stages a Dockerfile hunk is in, and which
  targets build on them) would narrow it; it isn't built yet.
- Import scanning counts every import, including ones under `if TYPE_CHECKING:`. That can add
  connection types but never drops one.

## The manifest

`scripts/impact_map_manifest.toml`:

| Section | What it holds |
|---|---|
| `packaging_paths`, `fail_open_paths` | Globs (`fnmatch`, `*` crosses `/`) for packaging files and for files that affect every image. |
| `[targets]` | Image target → platforms of its standing test agent. |
| `[requirements_files]` | Compiled requirements file → image targets that install it, as the `Dockerfile` does it. |
| `[connection_types]` | The connection-type vocabulary, with the areas each supports (from the type's REST v2 `job_types`: metadata/etl/reports → `metadata_collection`, `query_logs` → `query_logs`, `sql_query` → `monitors`). |
| `[modules]` | `apollo/` file or directory (trailing `/`) → `connection_types` and/or a `manual` note. Longest match wins. |
| `[packages]` | One entry per package in any `requirements*.in`. |

A `[packages]` entry:

| Key | Meaning |
|---|---|
| `scope` | `direct` (apollo uses it for specific connections or paths), `runtime` (platform/runtime-wide: web server, framework, agent base; the image checks and health checks cover it), `transitive` (pinned only to force a version; its mapping comes from what pulls it in), `dev` (tests/lint only). |
| `connection_types` | Types it serves, on top of what import scanning finds. |
| `imports` | Module names to scan `apollo/` for. Default: the name with `-` → `_`. Set it when they differ (`psycopg2-binary` → `psycopg2`, `pyjwt` → `jwt`); `[]` turns scanning off (e.g. `agent-base`, which provides `apollo.common`). |
| `areas` | For drivers: when this package's pin changes, which collection areas need a manual check. |
| `manual` | Extra checks, `[{ area = "other", reason = "..." }]`, reported whenever the package is in the `# via` chain. |

### Maintaining it

`tests/test_impact_map_manifest.py` fails when the manifest drifts, so the usual changes are:

- **New dependency in a `.in` file**: add a `[packages]` entry. Pick the scope; for a driver set
  `connection_types`, `imports` if the module name differs, and `areas`. Until it's added, the
  script fails open on any change to it and the test fails.
- **Dependency removed**: delete its entry (the test flags stale entries).
- **New connection type or integration module**: add the type to `[connection_types]` with its
  REST v2 name, and map the new module under `[modules]`.
- **New requirements file or Dockerfile stage**: update `[requirements_files]` (and `[targets]`
  for a new shipped target). The test parses the `Dockerfile` (`FROM ... AS`, `COPY
  requirements*.txt`, `COPY --from=`) and checks the mapping matches.
- **Module moved or renamed**: the test fails on `[modules]` keys that no longer exist.

## Tests

```bash
python -m pytest --noconftest tests/test_impact_map.py tests/test_impact_map_manifest.py
```

They need only `pytest` and `git`. `--noconftest` skips `tests/conftest.py`, which imports
apollo. `tests/test_impact_map.py` builds a throwaway git repo from `tests/impact_map/repo/`
(fixture requirements files, a `Dockerfile`, a few `apollo/` stubs) with
`tests/impact_map/manifest.toml`, commits a change on top, and checks the report. The fixture
`requirements*` files and `Dockerfile` are stored with a `.fixture` suffix, which the test drops
when it copies them, so dependency scanners (Dependabot, Aikido, Wiz) don't read their old pins
as the repo's own. The Docker
`tests` image has no `scripts/` or `.in` files, so both files skip there and CI runs them with a
bare Python instead.
