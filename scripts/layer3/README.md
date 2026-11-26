# Layer 3 CLIs

Layer 3 deploys a branch-built image to a real test agent on each cloud and checks it
through the Monte Carlo API. These scripts are the pieces a CI job runs in order:

```
agent_lease.py acquire   # one run per agent at a time (DynamoDB lease)
agent_lease.py hold &    # background: renew until the job ends
tfc_run.py apply         # Terraform Cloud run with agent_image=<release>, then <branch>
mc_check.py health       # agent reports the expected version / build / image tag
mc_check.py validate     # connection validations -> report.json
agent_lease.py release   # always
```

| script | needs | exit codes |
|---|---|---|
| `agent_lease.py` | Python >= 3.11, boto3, AWS credentials and region from the environment | 0 ok, 2 usage/AWS error, 3 agent busy past `--wait`, 4 lease lost |
| `tfc_run.py` | stdlib only; `TFC_TOKEN` | 0 ok, 2 usage/HTTP error, 5 run failed or canceled, 6 timeout (run canceled) |
| `mc_check.py` | stdlib only; `MCD_API_ID`, `MCD_API_TOKEN`, `MCD_API_BASE` (default dev API) | health: 0 match, 6 timeout; validate: 0 something validated and no FAIL, 1 any FAIL, 7 nothing validated (all BACKEND / NOT_COVERED / SKIP, or no types); 2 usage |

Run `<script> <command> --help` for the options.

`tfc_run.py` and `mc_check.py` use only the standard library on purpose: this repo is
public and the CI jobs run them with the bare CI Python, with no install step and no
dependency to pin or audit. pycarlo would cover the GraphQL health call, but not the
REST v2 `connections/{id}/validate` and `validations/{id}` endpoints (those are in the
separate `pycarlo2` package), and it brings requests, sgqlc and others with it.

The CI logs of this repo are public. What the scripts print:
- `agent_lease.py`: the agent id (not secret; the busy message names it), lease
  holder (job URL), branch and expiry.
- `tfc_run.py`: the run id and URL, statuses, variable and output names. Never variable
  or output values; outputs marked sensitive are left out of the outputs file.
- `mc_check.py`: health prints version, build and image tag only; validate prints
  connection types, validation names and outcomes only. No ids, raw responses, stack
  traces, `additional_data` or validation messages.
- `report.json` holds connection types, ids, outcomes and failed validation names.
  `--include-messages` adds the validations' friendly messages, which can contain hosts,
  users or account names: a report written with it must not be published.

Tests: `tests/test_agent_lease.py`, `tests/test_mc_check.py`, `tests/test_tfc_run.py`
(no network). Run them without the repo's conftest:

```
python -m pytest -p no:cacheprovider --noconftest tests/test_agent_lease.py \
  tests/test_mc_check.py tests/test_tfc_run.py
```
