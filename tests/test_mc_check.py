"""Tests for scripts/layer3/mc_check.py (recorded-shape fixtures, no network)."""

import importlib.util
import json
import sys
import threading
import urllib.error
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "layer3" / "mc_check.py"
if not SCRIPT.is_file():
    # The `tests` Docker stages copy tests/ but not scripts/; run-linter runs these.
    pytest.skip("scripts/layer3 is not in this checkout", allow_module_level=True)

spec = importlib.util.spec_from_file_location("layer3_mc_check", SCRIPT)
mc = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mc
spec.loader.exec_module(mc)

AGENT = "11111111-2222-3333-4444-555555555555"
BASE = "https://api.example.test"
SECRET = "do-not-log-this-value"

# Shapes follow monolith's TestCredentialsV2Response / ValidationRunOut and the agent's
# health_information(); values are made up.
HEALTH_RESPONSE = {
    "data": {
        "testAgentReachability": {
            "success": True,
            "additionalData": {
                "returnedData": {
                    "version": "1.2.3rc456",
                    "build": "456",
                    "platform": "AWS",
                    "env": {"MCD_SOME_SECRET": SECRET},
                    "platform_info": {
                        "image": "123.dkr.ecr.region.amazonaws.com/apollo-agent-ci:1.2.3rc456"
                    },
                    "trace_id": "abc",
                }
            },
        }
    }
}


def run_payload(status="completed", validations=None, run_id="run-1"):
    return {
        "id": run_id,
        "status": status,
        "revision": 3,
        "target_type": "connection",
        "target_id": "c",
        "validations_passed": 0,
        "validations_total": len(validations or []),
        "validations": validations or [],
    }


def validation(name, status="completed", passed=True, errors=None, warnings=None):
    return {
        "name": name,
        "description": "d",
        "status": status,
        "is_prerequisite": False,
        "passed": passed,
        "errors": errors or [],
        "warnings": warnings or [],
        "additional_data": {"secret": SECRET},
        "truncated": False,
    }


def issue(message):
    return {
        "friendly_message": message,
        "resolution": "r",
        "cause": "c",
        "stack_trace": f"Traceback ... {SECRET}",
    }


class FakeHttp:
    """Routes requests to queued responses per (method, path); records calls."""

    def __init__(self):
        self.routes: dict[tuple[str, str], list] = {}
        self.calls: list[tuple[str, str, dict, bytes | None]] = []
        self.lock = threading.Lock()

    def add(self, method, path, *responses):
        self.routes.setdefault((method, path), []).extend(responses)

    def __call__(self, method, url, headers, body=None, timeout=None):
        path = url[len(BASE) :]
        with self.lock:
            self.calls.append((method, path, headers, body))
            queue = self.routes[(method, path)]
            item = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(item, Exception):
            raise item
        status, payload, *rest = item
        headers_out = rest[0] if rest else {}
        return mc.Response(status, headers_out, json.dumps(payload).encode())


class Clock:
    def __init__(self):
        self.t = 0.0
        self.sleeps: list[float] = []
        self.lock = threading.Lock()

    def now(self):
        return self.t

    def sleep(self, seconds):
        with self.lock:
            self.sleeps.append(seconds)
            self.t += seconds


@pytest.fixture
def fake(monkeypatch):
    f = FakeHttp()
    monkeypatch.setattr(mc, "http", f)
    return f


@pytest.fixture
def clock(monkeypatch):
    c = Clock()
    monkeypatch.setattr(mc, "now", c.now)
    monkeypatch.setattr(mc, "sleep", c.sleep)
    return c


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("MCD_API_ID", "key-id")
    monkeypatch.setenv("MCD_API_TOKEN", "key-token")
    monkeypatch.setenv("MCD_API_BASE", BASE + "/")


# --------------------------------------------------------------------------- health


def health_args(*extra):
    return [
        "health",
        "--agent-id",
        AGENT,
        "--expect-version",
        "1.2.3rc456",
        "--expect-build",
        "456",
        *extra,
    ]


@pytest.mark.parametrize(
    "image, tag",
    [
        ("montecarlodata/agent:1.2.3-cloudrun", "1.2.3-cloudrun"),
        ("DOCKER|myacr.azurecr.io/agent:1.2.3rc4-azure", "1.2.3rc4-azure"),
        ("localhost:5000/agent:1.0", "1.0"),
        ("localhost:5000/agent", ""),
        ("repo/agent:1.0@sha256:abc", "1.0"),
    ],
)
def test_image_tag(image, tag):
    assert mc.image_tag(image) == tag


def test_health_match_prints_only_kept_fields(fake, clock, capsys):
    fake.add("POST", "/graphql", (200, HEALTH_RESPONSE))

    assert mc.main(health_args("--expect-image-tag", "1.2.3rc456")) == 0

    out = capsys.readouterr()
    assert "version=1.2.3rc456 build=456 image_tag=1.2.3rc456" in out.out
    assert SECRET not in out.out + out.err
    assert "ecr" not in out.out + out.err
    method, path, headers, body = fake.calls[0]
    assert headers["x-mcd-id"] == "key-id" and headers["x-mcd-token"] == "key-token"
    assert "Authorization" not in headers
    sent = json.loads(body)
    assert sent["variables"] == {"id": AGENT}
    assert 'validationName:"validate_agent_reachability"' in sent["query"]


def test_health_returned_data_as_json_string(fake, clock):
    payload = json.loads(json.dumps(HEALTH_RESPONSE))
    result = payload["data"]["testAgentReachability"]["additionalData"]
    result["returnedData"] = json.dumps(result["returnedData"])
    fake.add("POST", "/graphql", (200, payload))
    assert mc.main(health_args()) == 0


def old_version():
    payload = json.loads(json.dumps(HEALTH_RESPONSE))
    returned = payload["data"]["testAgentReachability"]["additionalData"][
        "returnedData"
    ]
    returned["version"], returned["build"] = "1.2.2", "400"
    return payload


def test_health_polls_until_upgraded(fake, clock, capsys):
    fake.add(
        "POST",
        "/graphql",
        (200, old_version()),
        mc.HttpError(502),
        (
            200,
            {
                "data": {
                    "testAgentReachability": {"success": False, "additionalData": None}
                }
            },
        ),
        (200, {"errors": [{"message": SECRET}]}),
        (200, HEALTH_RESPONSE),
    )
    assert mc.main(health_args("--poll", "15")) == 0
    out = capsys.readouterr().out
    assert "version=1.2.2 build=400" in out
    assert "health check: HTTP 502" in out
    assert "health check: agent not reachable" in out
    assert "health check: GraphQL errors: 1" in out
    assert SECRET not in out
    assert clock.sleeps == [15, 15, 15, 15]


def test_health_initial_wait(fake, clock):
    fake.add("POST", "/graphql", (200, HEALTH_RESPONSE))
    assert mc.main(health_args("--initial-wait", "45")) == 0
    assert clock.sleeps == [45]


def test_health_image_tag_mismatch_times_out_with_6(fake, clock, capsys):
    fake.add("POST", "/graphql", (200, HEALTH_RESPONSE))
    rc = mc.main(
        health_args("--expect-image-tag", "1.2.3rc456.post1", "--timeout", "60")
    )
    assert rc == 6
    assert "timed out after 60s" in capsys.readouterr().err


def test_health_network_error_keeps_polling(fake, clock):
    fake.add("POST", "/graphql", urllib.error.URLError("down"), (200, HEALTH_RESPONSE))
    assert mc.main(health_args()) == 0


def test_health_rejected_credentials_exit_2(fake, clock):
    fake.add("POST", "/graphql", mc.HttpError(401))
    assert mc.main(health_args()) == 2


def test_missing_credentials_exit_2(monkeypatch, fake):
    monkeypatch.delenv("MCD_API_TOKEN")
    assert mc.main(health_args()) == 2
    assert fake.calls == []


def test_default_base(monkeypatch):
    monkeypatch.delenv("MCD_API_BASE")
    assert mc.Api.from_env().base == "https://api.dev.getmontecarlo.com"


# --------------------------------------------------------------------------- validate


@pytest.fixture
def outputs(tmp_path):
    path = tmp_path / "out.json"
    path.write_text(
        json.dumps(
            {
                "run_id": "run-x",
                "status": "applied",
                "outputs": {
                    "platform": "aws",
                    "connection_ids": {
                        "postgres": "conn-pg",
                        "athena": "conn-at",
                        "glue": "conn-gl",
                        "sql-server": "conn-ss",
                    },
                },
            }
        )
    )
    return path


def validate_args(outputs, tmp_path, types, *extra):
    return [
        "validate",
        "--connection-ids",
        f"{outputs}:outputs.connection_ids",
        "--types",
        types,
        "--report",
        str(tmp_path / "report.json"),
        *extra,
    ]


def report(tmp_path):
    return json.loads((tmp_path / "report.json").read_text())


def accepted(run_id):
    return (202, run_payload("running", run_id=run_id), {"Retry-After": "3"})


def test_validate_classifies_and_writes_report(fake, clock, tmp_path, outputs, capsys):
    fake.add("POST", "/api/v2/connections/conn-pg/validate", accepted("r-pg"))
    fake.add(
        "GET",
        "/api/v2/validations/r-pg",
        (200, run_payload("running", [validation("a", status="running", passed=None)])),
        (200, run_payload("completed", [validation("a"), validation("b")])),
    )
    fake.add("POST", "/api/v2/connections/conn-at/validate", accepted("r-at"))
    fake.add(
        "GET",
        "/api/v2/validations/r-at",
        (
            200,
            run_payload(
                "completed",
                [
                    validation(
                        "query", passed=False, status="failed", errors=[issue("denied")]
                    ),
                    validation("later", status="skipped", passed=None),
                ],
            ),
        ),
    )
    fake.add("POST", "/api/v2/connections/conn-gl/validate", accepted("r-gl"))
    fake.add(
        "GET",
        "/api/v2/validations/r-gl",
        (200, run_payload("completed", [validation("perm", warnings=[issue("slow")])])),
    )
    fake.add("POST", "/api/v2/connections/conn-ss/validate", accepted("r-ss"))
    fake.add(
        "GET",
        "/api/v2/validations/r-ss",
        (
            200,
            run_payload("completed", [validation("x", status="skipped", passed=None)]),
        ),
    )

    rc = mc.main(
        validate_args(outputs, tmp_path, "postgres,athena,glue,sql-server,snowflake")
    )

    assert rc == 1
    data = report(tmp_path)
    assert data["platform"] == "aws"
    assert data["results"] == [
        {
            "connection_type": "postgres",
            "connection_id": "conn-pg",
            "outcome": "PASS",
            "failed_validations": [],
        },
        {
            "connection_type": "athena",
            "connection_id": "conn-at",
            "outcome": "FAIL",
            "failed_validations": [{"name": "query"}],
        },
        {
            "connection_type": "glue",
            "connection_id": "conn-gl",
            "outcome": "WARN",
            "failed_validations": [{"name": "perm"}],
        },
        {
            "connection_type": "sql-server",
            "connection_id": "conn-ss",
            "outcome": "SKIP",
            "failed_validations": [],
        },
        {
            "connection_type": "snowflake",
            "connection_id": None,
            "outcome": "NOT_COVERED",
            "failed_validations": [],
        },
    ]
    out = capsys.readouterr()
    everything = out.out + out.err + (tmp_path / "report.json").read_text()
    assert SECRET not in everything
    assert "stack_trace" not in everything and "additional_data" not in everything
    assert "conn-pg" not in out.out
    # Friendly messages can carry hosts and users: never on stdout, not in the report
    # unless asked for.
    assert "denied" not in everything and "slow" not in everything
    assert "athena: FAIL\n  - query: FAIL\n" in out.out
    assert "glue: WARN\n  - perm: WARN\n" in out.out
    assert "summary: FAIL=1 PASS=1 WARN=1 SKIP=1 BACKEND=0 NOT_COVERED=1" in out.out
    post = next(c for c in fake.calls if c[0] == "POST")
    assert post[2]["Authorization"] == "Bearer key-id:key-token"
    assert "x-mcd-id" not in post[2]
    assert 3 in clock.sleeps  # Retry-After from the 202


def test_validate_include_messages_puts_them_in_the_report_only(
    fake, clock, tmp_path, outputs, capsys
):
    fake.add("POST", "/api/v2/connections/conn-at/validate", accepted("r-at"))
    failed = validation(
        "query", passed=False, status="failed", errors=[issue("denied")]
    )
    fake.add(
        "GET", "/api/v2/validations/r-at", (200, run_payload("completed", [failed]))
    )
    args = validate_args(outputs, tmp_path, "athena", "--include-messages")
    assert mc.main(args) == 1
    (result,) = report(tmp_path)["results"]
    assert result["failed_validations"] == [
        {"name": "query", "friendly_message": "denied"}
    ]
    out = capsys.readouterr()
    assert "denied" not in out.out + out.err


def test_include_messages_help_warns_not_to_publish(capsys):
    with pytest.raises(SystemExit):
        mc.main(["validate", "--help"])
    help_text = " ".join(capsys.readouterr().out.split())
    assert "--include-messages" in help_text
    assert "must not be published" in help_text


def test_validate_only_warnings_exit_0(fake, clock, tmp_path, outputs):
    fake.add("POST", "/api/v2/connections/conn-gl/validate", accepted("r-gl"))
    fake.add(
        "GET",
        "/api/v2/validations/r-gl",
        (200, run_payload("completed", [validation("perm", warnings=[issue("slow")])])),
    )
    assert mc.main(validate_args(outputs, tmp_path, "glue,mysql")) == 0
    assert [r["outcome"] for r in report(tmp_path)["results"]] == [
        "WARN",
        "NOT_COVERED",
    ]


def test_validate_rate_limited_then_backend(fake, clock, tmp_path, outputs):
    fake.add(
        "POST",
        "/api/v2/connections/conn-pg/validate",
        mc.HttpError(429, retry_after=30),
    )
    args = validate_args(outputs, tmp_path, "postgres", "--include-messages")
    assert mc.main(args) == 7
    (result,) = report(tmp_path)["results"]
    assert result["outcome"] == "BACKEND"
    assert (
        result["failed_validations"][0]["friendly_message"]
        == "HTTP 429 after 3 retries"
    )
    assert clock.sleeps == [30, 30, 30]
    assert len(fake.calls) == 4


def test_validate_transient_errors_recover(fake, clock, tmp_path, outputs):
    fake.add(
        "POST",
        "/api/v2/connections/conn-pg/validate",
        mc.HttpError(503),
        urllib.error.URLError("reset"),
        accepted("r-pg"),
    )
    fake.add(
        "GET",
        "/api/v2/validations/r-pg",
        mc.HttpError(500),
        (200, run_payload("completed", [validation("a")])),
    )
    assert mc.main(validate_args(outputs, tmp_path, "postgres")) == 0
    assert report(tmp_path)["results"][0]["outcome"] == "PASS"
    assert clock.sleeps == [5, 15, 3, 5]


def test_validate_run_past_deadline_is_backend(fake, clock, tmp_path, outputs):
    fake.add("POST", "/api/v2/connections/conn-pg/validate", accepted("r-pg"))
    fake.add("GET", "/api/v2/validations/r-pg", (200, run_payload("running")))
    args = validate_args(
        outputs, tmp_path, "postgres", "--timeout", "10", "--include-messages"
    )
    assert mc.main(args) == 7
    (result,) = report(tmp_path)["results"]
    assert result["outcome"] == "BACKEND"
    assert "deadline" in result["failed_validations"][0]["friendly_message"]


def test_validate_conflict_is_fail(fake, clock, tmp_path, outputs):
    fake.add("POST", "/api/v2/connections/conn-pg/validate", mc.HttpError(409))
    assert mc.main(validate_args(outputs, tmp_path, "postgres")) == 1
    (result,) = report(tmp_path)["results"]
    assert result["outcome"] == "FAIL"
    assert result["failed_validations"] == [{"name": "(validate)"}]
    args = validate_args(outputs, tmp_path, "postgres", "--include-messages")
    assert mc.main(args) == 1
    assert report(tmp_path)["results"][0]["failed_validations"] == [
        {"name": "(validate)", "friendly_message": "API answered HTTP 409"}
    ]


def test_validate_empty_run_is_fail(fake, clock, tmp_path, outputs):
    fake.add(
        "POST", "/api/v2/connections/conn-pg/validate", (202, run_payload("completed"))
    )
    assert mc.main(validate_args(outputs, tmp_path, "postgres")) == 1


def test_validate_respects_concurrency(fake, clock, tmp_path, outputs, monkeypatch):
    active = {"now": 0, "max": 0}
    lock = threading.Lock()
    barrier_release = threading.Event()

    def tracked(api, connection_type, connection_id, timeout):
        with lock:
            active["now"] += 1
            active["max"] = max(active["max"], active["now"])
        barrier_release.wait(0.05)
        with lock:
            active["now"] -= 1
        return mc.Result(connection_type, connection_id, "PASS")

    monkeypatch.setattr(mc, "validate_one", tracked)
    types = "postgres,athena,glue,sql-server"
    assert mc.main(validate_args(outputs, tmp_path, types, "--concurrency", "2")) == 0
    assert active["max"] <= 2


def test_validate_no_types_writes_empty_report_and_exits_7(fake, tmp_path, outputs):
    assert mc.main(validate_args(outputs, tmp_path, "")) == 7
    assert report(tmp_path) == {"platform": "aws", "results": []}


@pytest.mark.parametrize("suffix", [":outputs.missing", ":outputs.platform"])
def test_validate_bad_connection_ids_path_exit_2(fake, tmp_path, outputs, suffix):
    args = validate_args(outputs, tmp_path, "postgres")
    args[2] = f"{outputs}{suffix}"
    assert mc.main(args) == 2


def test_validate_plain_map_file(fake, clock, tmp_path):
    path = tmp_path / "ids.json"
    path.write_text(json.dumps({"postgres": "conn-pg"}))
    args = ["validate", "--connection-ids", str(path), "--types", "mysql"]
    assert mc.main(args + ["--report", str(tmp_path / "report.json")]) == 7
    assert report(tmp_path) == {
        "platform": None,
        "results": [
            {
                "connection_type": "mysql",
                "connection_id": None,
                "outcome": "NOT_COVERED",
                "failed_validations": [],
            }
        ],
    }


def test_usage_error_exits_2():
    with pytest.raises(SystemExit) as exc:
        mc.main(["validate", "--types", "x"])
    assert exc.value.code == 2


def completed_with(*validations):
    return (200, run_payload("completed", list(validations)))


@pytest.mark.parametrize(
    "routes, rc",
    [
        # nothing validated: only BACKEND / NOT_COVERED / SKIP
        ({"conn-pg": mc.HttpError(503)}, 7),
        (
            {"conn-pg": completed_with(validation("x", status="skipped", passed=None))},
            7,
        ),
        # something validated: PASS or WARN wins over BACKEND
        ({"conn-pg": completed_with(validation("a")), "conn-at": mc.HttpError(503)}, 0),
        # FAIL always wins
        ({"conn-pg": mc.HttpError(409), "conn-at": mc.HttpError(503)}, 1),
    ],
)
def test_validate_nothing_validated_exits_7(
    fake, clock, tmp_path, outputs, capsys, routes, rc
):
    for conn, response in routes.items():
        if isinstance(response, Exception):
            fake.add("POST", f"/api/v2/connections/{conn}/validate", response)
        else:
            fake.add(
                "POST", f"/api/v2/connections/{conn}/validate", accepted(f"r-{conn}")
            )
            fake.add("GET", f"/api/v2/validations/r-{conn}", response)
    types = ",".join({"conn-pg": "postgres", "conn-at": "athena"}[c] for c in routes)
    assert mc.main(validate_args(outputs, tmp_path, types + ",snowflake")) == rc
    err = capsys.readouterr().err
    if rc == 7:
        (line,) = [l for l in err.splitlines() if l.startswith("nothing validated")]
        assert "NOT_COVERED=1" in line
    else:
        assert "nothing validated" not in err


@pytest.fixture
def terraform_outputs(tmp_path):
    """`terraform output -json` of a Layer 3 stack (made-up values)."""
    path = tmp_path / "out.json"
    path.write_text(
        json.dumps(
            {
                "agent_id": {
                    "sensitive": False,
                    "type": "string",
                    "value": "agent-uuid",
                },
                "platform": {"sensitive": False, "type": "string", "value": "aws"},
                "image_variant": {
                    "sensitive": False,
                    "type": "string",
                    "value": "lambda",
                },
                "health_timeout_seconds": {
                    "sensitive": False,
                    "type": "number",
                    "value": 600,
                },
                "connection_ids": {
                    "sensitive": False,
                    "type": ["map", "string"],
                    "value": {"postgres": "conn-pg", "athena": "conn-at"},
                },
            }
        )
    )
    return path


def test_validate_reads_terraform_output_json(fake, clock, tmp_path, terraform_outputs):
    fake.add("POST", "/api/v2/connections/conn-pg/validate", accepted("r-pg"))
    fake.add("GET", "/api/v2/validations/r-pg", completed_with(validation("a")))
    args = [
        "validate",
        "--connection-ids",
        f"{terraform_outputs}:connection_ids.value",
        "--types",
        "postgres,snowflake",
        "--report",
        str(tmp_path / "report.json"),
    ]
    assert mc.main(args) == 0
    data = report(tmp_path)
    assert data["platform"] == "aws"
    assert [
        (r["connection_type"], r["connection_id"], r["outcome"])
        for r in data["results"]
    ] == [
        ("postgres", "conn-pg", "PASS"),
        ("snowflake", None, "NOT_COVERED"),
    ]


def test_validate_terraform_output_without_value_is_usage_error(
    fake, tmp_path, terraform_outputs
):
    # The output object itself (value/type/sensitive) is not a type -> id map.
    args = validate_args(terraform_outputs, tmp_path, "postgres")
    args[2] = f"{terraform_outputs}:connection_ids"
    assert mc.main(args) == 2
