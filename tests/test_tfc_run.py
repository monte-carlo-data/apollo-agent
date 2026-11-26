"""Tests for scripts/layer3/tfc_run.py (fake HTTP, no network)."""

import importlib.util
import json
import sys
import urllib.error
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "layer3" / "tfc_run.py"
if not SCRIPT.is_file():
    # The `tests` Docker stages copy tests/ but not scripts/; run-linter runs these.
    pytest.skip("scripts/layer3 is not in this checkout", allow_module_level=True)

spec = importlib.util.spec_from_file_location("layer3_tfc_run", SCRIPT)
tfc = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = tfc
spec.loader.exec_module(tfc)

API = "https://app.terraform.io/api/v2"
ORG = "example-org"
WS = "apollo-agent-ci-aws"
WS_ID = "ws-abc123"
RUN_ID = "run-xyz789"
TOKEN = "tfc-test-token"
IMAGE = "registry.example/apollo-agent-ci:1.2.3rc456"
SECRET = "sensitive-output-value"

WORKSPACE = {"data": {"id": WS_ID, "type": "workspaces", "attributes": {"name": WS}}}
RUN_CREATED = {
    "data": {"id": RUN_ID, "type": "runs", "attributes": {"status": "pending"}}
}


def run(status):
    return {"data": {"id": RUN_ID, "type": "runs", "attributes": {"status": status}}}


def output(name, value, sensitive=False):
    return {
        "id": f"wsout-{name}",
        "type": "state-version-outputs",
        "attributes": {
            "name": name,
            "sensitive": sensitive,
            "type": "string",
            "value": None if sensitive and value is None else value,
        },
    }


OUTPUTS = {
    "data": [
        output("platform", "aws"),
        output("agent_id", "agent-uuid"),
        output("health_timeout_seconds", 600),
        output("connection_ids", {"athena": "conn-at", "postgres": "conn-pg"}),
        output("token_secret", SECRET, sensitive=True),
    ],
    "links": {"next": None},
}


class FakeHttp:
    def __init__(self):
        self.routes: dict[tuple[str, str], list] = {}
        self.calls: list[tuple[str, str, dict, bytes | None]] = []

    def add(self, method, path, *responses):
        self.routes.setdefault((method, path), []).extend(responses)

    def __call__(self, method, url, headers, body=None):
        assert url.startswith(API + "/")
        path = url[len(API) :]
        self.calls.append((method, path, headers, body))
        queue = self.routes[(method, path)]
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(item, Exception):
            raise item
        return json.loads(json.dumps(item))

    def bodies(self, method, path):
        return [json.loads(c[3]) for c in self.calls if c[0] == method and c[1] == path]


class Clock:
    def __init__(self):
        self.t = 0.0
        self.sleeps: list[float] = []

    def now(self):
        return self.t

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.t += seconds


@pytest.fixture
def fake(monkeypatch):
    f = FakeHttp()
    monkeypatch.setattr(tfc, "http", f)
    return f


@pytest.fixture
def clock(monkeypatch):
    c = Clock()
    monkeypatch.setattr(tfc, "now", c.now)
    monkeypatch.setattr(tfc, "sleep", c.sleep)
    return c


@pytest.fixture(autouse=True)
def token(monkeypatch):
    monkeypatch.setenv("TFC_TOKEN", TOKEN)


def apply_args(out, *extra):
    return [
        "apply",
        "--org",
        ORG,
        "--workspace",
        WS,
        "--var",
        f"agent_image={IMAGE}",
        "--message",
        "https://ci.example/job/1",
        "--outputs-file",
        str(out),
        *extra,
    ]


def happy_routes(fake, *statuses):
    fake.add("GET", f"/organizations/{ORG}/workspaces/{WS}", WORKSPACE)
    fake.add("POST", "/runs", RUN_CREATED)
    fake.add("GET", f"/runs/{RUN_ID}", *[run(s) for s in statuses])
    fake.add("GET", f"/workspaces/{WS_ID}/current-state-version-outputs", OUTPUTS)


def test_apply_creates_run_waits_and_writes_outputs(fake, clock, tmp_path, capsys):
    out = tmp_path / "out.json"
    happy_routes(fake, "pending", "planning", "applying", "applied")

    assert tfc.main(apply_args(out, "--poll", "10")) == 0

    (body,) = fake.bodies("POST", "/runs")
    assert body == {
        "data": {
            "type": "runs",
            "attributes": {
                "message": "https://ci.example/job/1",
                "auto-apply": True,
                "variables": [{"key": "agent_image", "value": f'"{IMAGE}"'}],
            },
            "relationships": {
                "workspace": {"data": {"type": "workspaces", "id": WS_ID}}
            },
        }
    }
    for _, _, headers, _ in fake.calls:
        assert headers["Authorization"] == f"Bearer {TOKEN}"
        assert headers["Content-Type"] == "application/vnd.api+json"
    written = json.loads(out.read_text())
    assert written == {
        "run_id": RUN_ID,
        "run_url": f"https://app.terraform.io/app/{ORG}/workspaces/{WS}/runs/{RUN_ID}",
        "status": "applied",
        "outputs": {
            "platform": "aws",
            "agent_id": "agent-uuid",
            "health_timeout_seconds": 600,
            "connection_ids": {"athena": "conn-at", "postgres": "conn-pg"},
        },
    }
    assert clock.sleeps == [10, 10, 10]
    logged = capsys.readouterr()
    text = logged.out + logged.err
    for value in (TOKEN, IMAGE, SECRET, "agent-uuid", "conn-pg"):
        assert value not in text
    assert "variables: agent_image" in logged.out
    assert "token_secret is sensitive" in logged.err


def test_apply_planned_and_finished_is_success(fake, clock, tmp_path):
    out = tmp_path / "out.json"
    happy_routes(fake, "planned_and_finished")
    assert tfc.main(apply_args(out)) == 0
    assert json.loads(out.read_text())["status"] == "planned_and_finished"


@pytest.mark.parametrize(
    "status",
    ["errored", "discarded", "canceled", "force_canceled", "policy_soft_failed"],
)
def test_apply_failed_run_exits_5_with_url_only(fake, clock, tmp_path, capsys, status):
    out = tmp_path / "out.json"
    happy_routes(fake, "planning", status)
    assert tfc.main(apply_args(out)) == 5
    assert not out.exists()
    err = capsys.readouterr().err
    assert (
        f"run {status}: https://app.terraform.io/app/{ORG}/workspaces/{WS}/runs/{RUN_ID}"
        in err
    )


def test_apply_timeout_cancels_and_exits_6(fake, clock, tmp_path):
    out = tmp_path / "out.json"
    happy_routes(fake, "applying")
    fake.add("POST", f"/runs/{RUN_ID}/actions/cancel", None)

    assert tfc.main(apply_args(out, "--timeout", "30", "--poll", "10")) == 6
    assert not out.exists()
    assert fake.bodies("POST", f"/runs/{RUN_ID}/actions/cancel") == [
        {"comment": "tfc_run.py timeout"}
    ]
    assert ("POST", f"/runs/{RUN_ID}/actions/discard") not in {
        c[:2] for c in fake.calls
    }


def test_apply_timeout_on_queued_run_discards(fake, clock, tmp_path):
    out = tmp_path / "out.json"
    happy_routes(fake, "pending")
    fake.add("POST", f"/runs/{RUN_ID}/actions/cancel", tfc.HttpError(409))
    fake.add("POST", f"/runs/{RUN_ID}/actions/discard", None)

    assert tfc.main(apply_args(out, "--timeout", "10")) == 6
    assert len(fake.bodies("POST", f"/runs/{RUN_ID}/actions/discard")) == 1


def test_transient_errors_are_retried(fake, clock, tmp_path):
    out = tmp_path / "out.json"
    happy_routes(fake, "applied")
    fake.routes[("GET", f"/organizations/{ORG}/workspaces/{WS}")] = [
        tfc.HttpError(502),
        urllib.error.URLError("reset"),
        tfc.HttpError(429, retry_after=7),
        WORKSPACE,
    ]
    assert tfc.main(apply_args(out)) == 0
    assert clock.sleeps[:3] == [5, 15, 7]


def test_persistent_server_error_exits_2(fake, clock, tmp_path, capsys):
    fake.add("GET", f"/organizations/{ORG}/workspaces/{WS}", tfc.HttpError(503))
    assert tfc.main(apply_args(tmp_path / "out.json")) == 2
    assert "HTTP 503 after 3 retries" in capsys.readouterr().err
    assert len(fake.calls) == 4


def test_client_error_is_not_retried(fake, clock, tmp_path, capsys):
    fake.add("GET", f"/organizations/{ORG}/workspaces/{WS}", tfc.HttpError(404))
    assert tfc.main(apply_args(tmp_path / "out.json")) == 2
    assert len(fake.calls) == 1
    err = capsys.readouterr().err
    assert "HTTP 404" in err and TOKEN not in err


def test_missing_token_exits_2(monkeypatch, fake, tmp_path):
    monkeypatch.delenv("TFC_TOKEN")
    assert tfc.main(apply_args(tmp_path / "out.json")) == 2
    assert fake.calls == []


def test_outputs_command_follows_pages(fake, tmp_path):
    out = tmp_path / "out.json"
    fake.add("GET", f"/organizations/{ORG}/workspaces/{WS}", WORKSPACE)
    page1 = {
        "data": [output("platform", "aws")],
        "links": {
            "next": f"{API}/workspaces/{WS_ID}/current-state-version-outputs?page=2"
        },
    }
    page2 = {"data": [output("agent_id", "agent-uuid")], "links": {"next": None}}
    fake.add("GET", f"/workspaces/{WS_ID}/current-state-version-outputs", page1)
    fake.add("GET", f"/workspaces/{WS_ID}/current-state-version-outputs?page=2", page2)

    args = ["outputs", "--org", ORG, "--workspace", WS, "--outputs-file", str(out)]
    assert tfc.main(args) == 0
    assert json.loads(out.read_text()) == {
        "outputs": {"platform": "aws", "agent_id": "agent-uuid"}
    }


def test_next_page_off_api_is_refused(fake, tmp_path):
    fake.add("GET", f"/organizations/{ORG}/workspaces/{WS}", WORKSPACE)
    page = {"data": [], "links": {"next": "https://elsewhere.example/steal"}}
    fake.add("GET", f"/workspaces/{WS_ID}/current-state-version-outputs", page)
    args = [
        "outputs",
        "--org",
        ORG,
        "--workspace",
        WS,
        "--outputs-file",
        str(tmp_path / "o"),
    ]
    assert tfc.main(args) == 2


def test_multiple_vars_and_hcl_encoding(fake, clock, tmp_path):
    happy_routes(fake, "applied")
    args = apply_args(tmp_path / "out.json", "--var", 'note=has "quotes"=and equals')
    assert tfc.main(args) == 0
    (body,) = fake.bodies("POST", "/runs")
    assert body["data"]["attributes"]["variables"] == [
        {"key": "agent_image", "value": f'"{IMAGE}"'},
        {"key": "note", "value": '"has \\"quotes\\"=and equals"'},
    ]


@pytest.mark.parametrize("bad", ["novalue", "=x"])
def test_bad_var_is_usage_error(tmp_path, bad, capsys):
    with pytest.raises(SystemExit) as exc:
        tfc.main(apply_args(tmp_path / "out.json", "--var", bad))
    assert exc.value.code == 2


def test_usage_error_exits_2():
    with pytest.raises(SystemExit) as exc:
        tfc.main(["apply", "--org", ORG])
    assert exc.value.code == 2
