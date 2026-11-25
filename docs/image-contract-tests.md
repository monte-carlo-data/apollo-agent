# Image contract tests

The agent images run in customers' infrastructure, so a dependency bump that breaks an
image must fail CI before merge, with a message a person or the orchestrator bot can act
on. These checks run against a built image, with the image's own interpreter, and are the
same everywhere they run (YET-3659, layer 1).

## What is checked

`scripts/image_checks/check_image.py --target <target> --image <image>` runs, in fresh
containers with no network:

| Check | What it does |
|---|---|
| `version` | `apollo/agent/settings.py` prints the expected `<code_version>,<build_number>`. |
| `imports` | Imports every native driver and compiled library the agent uses (`oracledb`, `pyodbc`, `ibm_db`, `teradatasql`, `psycopg2`, `snowflake.connector`, `hdbcli`, `duckdb`, `pyarrow`, ... see `NATIVE_MODULES`) and the target's entry modules. Also loads the Oracle Instant Client (thick mode) and checks that a SQL Server ODBC driver (17 or 18) is registered. |
| `ldd` | Runs `ldd` (or the dynamic loader's `--list`) on every shared object on the interpreter's path, `/opt/oracle/instantclient` and `/opt/microsoft`; fails on any `not found`. Known, harmless gaps are listed with a reason in `LDD_IGNORED`. |
| `health` | Starts the entrypoint and hits health: gunicorn on `/api/v1/test/health` for `generic` (and the `aws_proxied` entry module, run in the `generic` image), `aws_proxied` and `cloudrun`; the Lambda handler from the image's `CMD`, called with an API Gateway event; for `azure`, the image's own Functions host entrypoint is started with an [Azurite](https://github.com/Azure/Azurite) storage emulator (Durable Functions needs storage) and a pre-seeded placeholder function key, and health is called through the host. Only `version` and `build` are read from the health response. |

Entry modules per target: `generic`: `apollo.interfaces.generic.main`,
`apollo.interfaces.aws.main`; `cloudrun`: `apollo.interfaces.cloudrun.main`; `lambda`:
`apollo.interfaces.lambda_function.handler`; `azure`: `function_app`.

Placeholders the checks provide, because the real values only exist in a customer's
deployment: an `authorized_user` GCP credentials file and `GOOGLE_CLOUD_PROJECT` for
`cloudrun` (its main module builds a logging client at import), a zero
`APPLICATIONINSIGHTS_CONNECTION_STRING`, Azurite and a function key for `azure`, and `AWS_REGION` for `lambda`.

### Reading a failure

Each failure is one line naming the check, the module or library, and the image, followed
by the underlying error, indented:

```
IMAGE CHECK FAILED: pyodbc import failed in the lambda image
    ImportError: libodbc.so.2: cannot open shared object file: No such file or directory
      File "/var/task/pyodbc/__init__.py", ...
```

The job ends with a summary of every failed check for that image. `grep 'IMAGE CHECK FAILED'`
finds them all.

## Where they run

| Pipeline | Images checked |
|---|---|
| Feature branch, no packaging change | none: no image is built |
| Feature branch, packaging change (`requirements*`, `Dockerfile`, `.dockerignore`, `resources/`) | the targets `scripts/impact_map.py` selects, built locally by `image-check-<target>` jobs (never pushed). If the impact map is missing, fails or prints something unexpected: all four. |
| Merge queue (`gh-readonly-queue/*`), `dev` | every image the pipeline builds (`azure`, `cloudrun`, `aws_proxied`, `lambda`), inside its build job, before the push |
| `v*` tags | every image the release builds, the same way |

`build-ok` requires the branch's `image-check-<target>` jobs, so a broken image fails the
required check. `aws_proxied` is not built on branches: it is `generic` with a different
`CMD`, so `generic`'s checks cover its entry module and start it.

### How the pipeline is put together

CircleCI dynamic config (project setting "Enable dynamic config using setup workflows"):

1. `.circleci/config.yml` is the setup config. Its `setup` job runs
   `scripts/image_checks/pipeline_setup.py`, which renders `.circleci/continue_config.yml`
   and continues the pipeline with it. All the real jobs live in `continue_config.yml`.
2. In `continue_config.yml`, the branch image jobs sit between
   `# image-check:begin <target>` / `# image-check:end <target>` markers, and lines tagged
   `# image-check:<target>` (in `build-ok`'s `requires`). The setup job drops the ones for
   targets it didn't select. Tags, `dev`, `main` and the merge queue get none of them, so
   their workflows are exactly what they were before dynamic config.
3. The setup job also posts the impact map's `--format markdown` manual checklist as one
   sticky PR comment (`scripts/image_checks/pr_comment.py`). It never fails the build, and
   it is a no-op until a token is provided in `IMPACT_MAP_GITHUB_TOKEN`. The setup job runs
   the branch's own scripts, so whatever token is added there is readable by any branch
   that can trigger a build: use a fine-grained token limited to this repository with only
   pull-request write access.

A change to the checks themselves (`check_image.py`, `in_image.py`) builds and checks all
four targets on the branch.

## Running locally

Needs Docker. Images are built for `linux/amd64` (emulated on Apple Silicon, so slower).

```bash
make image-check TARGET=lambda
```

```bash
make image-checks
```

Check an image you already built, or run a subset of checks:

```bash
python3 scripts/image_checks/check_image.py --target azure --image my-azure:tag --checks imports,ldd
```

Unit tests for the scripts (they also run in the `run-linter` job, since the `tests` Docker
stages don't copy `scripts/`):

```bash
make test-image-checks
```

## Adding to the checks

- A new native driver: add its module to `NATIVE_MODULES` in `check_image.py`.
- A new image target: add a `Target` to `TARGETS`, a marker block and `build-ok` line in
  `continue_config.yml`, and the name to `TARGETS` in `pipeline_setup.py`.
- A library `ldd` can't resolve and that is never loaded: add it to `LDD_IGNORED` with the
  reason. Don't ignore a library a driver needs.
