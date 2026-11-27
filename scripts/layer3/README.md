# Layer 3 CLIs

Layer 3 deploys a branch-built image to a real test agent on each cloud and checks it
through the Monte Carlo API. A CI job runs these steps in the agent's Terraform stack
(Terraform Cloud only stores the state; Terraform runs in the job):

```
agent_lease.py acquire --table T --agent-id ID --holder "$CIRCLE_BUILD_URL" --out /tmp/lease.json
agent_lease.py hold --table T --lease /tmp/lease.json &     # background: renew until the job ends
terraform init
terraform apply -auto-approve -var agent_image=<release ref>   # reset
terraform apply -auto-approve -var agent_image=<branch ref>    # upgrade
terraform output -json > out.json
mc_check.py health --agent-id ID --expect-version V --expect-build N --expect-image-tag TAG
mc_check.py validate --connection-ids out.json:connection_ids.value --types postgres,athena \
  --report report.json
agent_lease.py release --table T --lease /tmp/lease.json    # always
```

`--connection-ids FILE[:PATH]` takes a dotted path into any JSON file; with `terraform
output -json` the map is under `connection_ids.value`, and the report's `platform` comes
from the `platform` output in the same file.

| script | needs | exit codes |
|---|---|---|
| `agent_lease.py` | Python >= 3.11, boto3, AWS credentials and region from the environment | 0 ok, 2 usage/AWS error, 3 agent busy past `--wait`, 4 lease lost |
| `mc_check.py` | stdlib only; `MCD_API_ID`, `MCD_API_TOKEN`, `MCD_API_BASE` (default dev API) | health: 0 match, 6 timeout; validate: 0 something validated and no FAIL, 1 any FAIL, 7 nothing validated (all BACKEND / NOT_COVERED / SKIP, or no types); 2 usage |

Run `<script> <command> --help` for the options.

`mc_check.py` uses only the standard library on purpose: this repo is public and the CI
jobs run it with the bare CI Python, with no install step and no dependency to pin or
audit. pycarlo would cover the GraphQL health call, but not the REST v2
`connections/{id}/validate` and `validations/{id}` endpoints (those are in the separate
`pycarlo2` package), and it brings requests, sgqlc and others with it.

The CI logs of this repo are public. What the scripts print:
- `agent_lease.py`: the agent id (not secret; the busy message names it), lease
  holder (job URL), branch and expiry.
- `mc_check.py`: health prints version, build and image tag only; validate prints
  connection types, validation names and outcomes only. No ids, raw responses, stack
  traces, `additional_data` or validation messages.
- `report.json` holds connection types, ids, outcomes and failed validation names.
  `--include-messages` adds the validations' friendly messages, which can contain hosts,
  users or account names: a report written with it must not be published.

Tests: `tests/test_agent_lease.py`, `tests/test_mc_check.py` (no network), run by
`run-linter`. Locally, without the repo's conftest:

```
python -m pytest -p no:cacheprovider --noconftest tests/test_agent_lease.py tests/test_mc_check.py
```
