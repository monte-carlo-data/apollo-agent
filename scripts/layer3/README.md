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
| `mc_check.py` | stdlib only; `MCD_API_ID`, `MCD_API_TOKEN`, `MCD_API_BASE` (default dev API) | health: 0 match, 6 timeout; validate: 0 no FAIL, 1 any FAIL; 2 usage |

Run `<script> <command> --help` for the options.

The CI logs of this repo are public. The scripts print statuses, run URLs, connection
types and validation messages only: no tokens, variable or output values, agent or
connection ids, raw API responses, stack traces or `additional_data`.

Tests: `tests/test_agent_lease.py`, `tests/test_mc_check.py`, `tests/test_tfc_run.py`
(no network). Run them without the repo's conftest:

```
python -m pytest -p no:cacheprovider --noconftest tests/test_agent_lease.py \
  tests/test_mc_check.py tests/test_tfc_run.py
```
