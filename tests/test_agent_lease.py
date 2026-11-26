"""Tests for scripts/layer3/agent_lease.py (botocore Stubber, no network)."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "layer3" / "agent_lease.py"
if not SCRIPT.is_file():
    # The `tests` Docker stages copy tests/ but not scripts/; run-linter runs these.
    pytest.skip("scripts/layer3 is not in this checkout", allow_module_level=True)

import boto3  # noqa: E402  (after the skip: the Docker stages have boto3, a bare venv may not)
from botocore.stub import ANY, Stubber  # noqa: E402

spec = importlib.util.spec_from_file_location("layer3_agent_lease", SCRIPT)
lease_mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = lease_mod
spec.loader.exec_module(lease_mod)

TABLE = "leases"
AGENT = "11111111-2222-3333-4444-555555555555"
HOLDER = "https://ci.example/job/1"
T0 = 1_764_090_000


class Clock:
    def __init__(self, start: int = T0):
        self.t = start
        self.sleeps: list[float] = []

    def now(self) -> int:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += int(seconds)


@pytest.fixture
def clock(monkeypatch):
    c = Clock()
    monkeypatch.setattr(lease_mod, "now", c.now)
    monkeypatch.setattr(lease_mod, "sleep", c.sleep)
    return c


@pytest.fixture
def client():
    c = boto3.client(
        "dynamodb",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    with Stubber(c) as stubber:
        c.stubber = stubber
        yield c
        stubber.assert_no_pending_responses()


def put_params(at: int, ttl: int = 600, branch: str = "feat/x"):
    return {
        "TableName": TABLE,
        "Item": {
            "agent_id": {"S": AGENT},
            "lease_token": {"S": ANY},
            "holder": {"S": HOLDER},
            "branch": {"S": branch},
            "acquired_at": {"N": str(at)},
            "expires_at": {"N": str(at + ttl)},
            "ttl": {"N": str(at + ttl + 86400)},
        },
        "ConditionExpression": "attribute_not_exists(agent_id) OR expires_at < :now",
        "ExpressionAttributeValues": {":now": {"N": str(at)}},
    }


def get_params():
    return {
        "TableName": TABLE,
        "Key": {"agent_id": {"S": AGENT}},
        "ConsistentRead": True,
        "ProjectionExpression": "holder, branch, expires_at",
    }


def held_item(expires_at: int):
    return {
        "Item": {
            "holder": {"S": "https://ci.example/job/other"},
            "branch": {"S": "other-branch"},
            "expires_at": {"N": str(expires_at)},
        }
    }


def condition_failed(stubber, method):
    stubber.add_client_error(
        method,
        service_error_code="ConditionalCheckFailedException",
        http_status_code=400,
    )


def write_lease(path: Path, token: str = "tok-1", expires_at: int = T0 + 600):
    path.write_text(
        json.dumps(
            {
                "agent_id": AGENT,
                "lease_token": token,
                "expires_at": expires_at,
                "holder": HOLDER,
            }
        )
    )


def acquire_args(out: Path, *extra: str):
    return [
        "acquire",
        "--table",
        TABLE,
        "--agent-id",
        AGENT,
        "--holder",
        HOLDER,
        "--branch",
        "feat/x",
        "--out",
        str(out),
        *extra,
    ]


def test_acquire_free_agent_writes_lease_file(tmp_path, clock, client):
    out = tmp_path / "lease.json"
    (tmp_path / "lease.lost").write_text("stale\n")
    client.stubber.add_response("put_item", {}, put_params(T0))

    assert lease_mod.main(acquire_args(out), client=client) == 0

    lease = json.loads(out.read_text())
    assert set(lease) == {"agent_id", "lease_token", "expires_at", "holder"}
    assert lease["agent_id"] == AGENT
    assert lease["holder"] == HOLDER
    assert lease["expires_at"] == T0 + 600
    assert len(lease["lease_token"]) == 36
    assert not (tmp_path / "lease.lost").exists()


def test_acquire_waits_for_busy_agent_then_gets_it(tmp_path, clock, client, capsys):
    out = tmp_path / "lease.json"
    condition_failed(client.stubber, "put_item")
    client.stubber.add_response("get_item", held_item(T0 + 100), get_params())
    client.stubber.add_response("put_item", {}, put_params(T0 + 30))

    assert lease_mod.main(acquire_args(out, "--poll", "30"), client=client) == 0
    assert clock.sleeps == [30]
    assert "waiting: agent" in capsys.readouterr().err


def test_acquire_busy_past_wait_exits_3_with_holder(tmp_path, clock, client, capsys):
    out = tmp_path / "lease.json"
    for i in range(3):
        condition_failed(client.stubber, "put_item")
        client.stubber.add_response("get_item", held_item(T0 + 3600), get_params())

    rc = lease_mod.main(
        acquire_args(out, "--wait", "60", "--poll", "30"), client=client
    )

    assert rc == 3
    assert not out.exists()
    last = capsys.readouterr().err.strip().splitlines()[-1]
    assert last == (
        f"agent {AGENT} busy, held by https://ci.example/job/other (branch other-branch) "
        "until 2025-11-25T18:00:00Z"
    )


def test_acquire_retries_at_once_when_item_vanished(tmp_path, clock, client):
    out = tmp_path / "lease.json"
    condition_failed(client.stubber, "put_item")
    client.stubber.add_response("get_item", {}, get_params())
    client.stubber.add_response("put_item", {}, put_params(T0))

    assert lease_mod.main(acquire_args(out), client=client) == 0
    assert clock.sleeps == []


def test_acquire_aws_error_exits_2(tmp_path, clock, client, capsys):
    client.stubber.add_client_error(
        "put_item", service_error_code="AccessDeniedException", http_status_code=400
    )
    assert lease_mod.main(acquire_args(tmp_path / "lease.json"), client=client) == 2
    assert "AccessDeniedException" in capsys.readouterr().err


def renew_params(token: str, expires_at: int):
    return {
        "TableName": TABLE,
        "Key": {"agent_id": {"S": AGENT}},
        "UpdateExpression": "SET expires_at = :new, #ttl = :ttl",
        "ConditionExpression": "lease_token = :tok",
        "ExpressionAttributeNames": {"#ttl": "ttl"},
        "ExpressionAttributeValues": {
            ":new": {"N": str(expires_at)},
            ":ttl": {"N": str(expires_at + 86400)},
            ":tok": {"S": token},
        },
    }


def test_renew_extends_and_updates_lease_file(tmp_path, clock, client):
    path = tmp_path / "lease.json"
    write_lease(path)
    clock.t = T0 + 120
    client.stubber.add_response("update_item", {}, renew_params("tok-1", T0 + 720))

    args = ["renew", "--table", TABLE, "--lease", str(path)]
    assert lease_mod.main(args, client=client) == 0
    assert json.loads(path.read_text())["expires_at"] == T0 + 720


def test_renew_lost_exits_4_and_writes_marker(tmp_path, clock, client):
    path = tmp_path / "lease.json"
    write_lease(path)
    condition_failed(client.stubber, "update_item")

    args = ["renew", "--table", TABLE, "--lease", str(path)]
    assert lease_mod.main(args, client=client) == 4
    assert (tmp_path / "lease.lost").exists()


def test_renew_without_lease_file_exits_2(tmp_path, client):
    args = ["renew", "--table", TABLE, "--lease", str(tmp_path / "missing.json")]
    assert lease_mod.main(args, client=client) == 2


def test_hold_renews_each_interval(tmp_path, clock, client):
    path = tmp_path / "lease.json"
    write_lease(path)
    client.stubber.add_response("update_item", {}, renew_params("tok-1", T0 + 60 + 600))
    client.stubber.add_response(
        "update_item", {}, renew_params("tok-1", T0 + 120 + 600)
    )

    assert lease_mod.hold(client, TABLE, path, interval=60, ttl=600, max_rounds=2) == 0
    assert clock.sleeps == [60, 60]
    assert json.loads(path.read_text())["expires_at"] == T0 + 720


def test_hold_loses_lease_writes_marker_and_exits_4(tmp_path, clock, client):
    path = tmp_path / "lease.json"
    write_lease(path)
    client.stubber.add_response("update_item", {}, renew_params("tok-1", T0 + 660))
    condition_failed(client.stubber, "update_item")

    assert lease_mod.hold(client, TABLE, path, interval=60, ttl=600) == 4
    assert (tmp_path / "lease.lost").exists()
    check_args = ["check", "--lease", str(path)]
    assert lease_mod.main(check_args) == 4


def test_hold_tolerates_transient_error_until_expiry(tmp_path, clock, client):
    path = tmp_path / "lease.json"
    write_lease(path, expires_at=T0 + 150)
    for _ in range(3):
        client.stubber.add_client_error(
            "update_item",
            service_error_code="InternalServerError",
            http_status_code=500,
        )

    # 60 and 120 are before expiry (retry), 180 is past it (lost).
    assert lease_mod.hold(client, TABLE, path, interval=60, ttl=600) == 4
    assert "could not renew" in (tmp_path / "lease.lost").read_text()


def delete_params(token: str):
    return {
        "TableName": TABLE,
        "Key": {"agent_id": {"S": AGENT}},
        "ConditionExpression": "lease_token = :tok",
        "ExpressionAttributeValues": {":tok": {"S": token}},
    }


def test_release_deletes_own_lease(tmp_path, client):
    path = tmp_path / "lease.json"
    write_lease(path)
    client.stubber.add_response("delete_item", {}, delete_params("tok-1"))
    assert (
        lease_mod.main(
            ["release", "--table", TABLE, "--lease", str(path)], client=client
        )
        == 0
    )


def test_release_of_lost_lease_warns_and_exits_0(tmp_path, client, capsys):
    path = tmp_path / "lease.json"
    write_lease(path)
    condition_failed(client.stubber, "delete_item")
    assert (
        lease_mod.main(
            ["release", "--table", TABLE, "--lease", str(path)], client=client
        )
        == 0
    )
    assert "already lost" in capsys.readouterr().err


def test_release_without_lease_file_is_a_no_op(tmp_path, client):
    path = tmp_path / "lease.json"
    assert (
        lease_mod.main(
            ["release", "--table", TABLE, "--lease", str(path)], client=client
        )
        == 0
    )


def test_release_aws_error_exits_2(tmp_path, client):
    path = tmp_path / "lease.json"
    write_lease(path)
    client.stubber.add_client_error(
        "delete_item", service_error_code="AccessDeniedException", http_status_code=400
    )
    assert (
        lease_mod.main(
            ["release", "--table", TABLE, "--lease", str(path)], client=client
        )
        == 2
    )


def test_check_without_marker_exits_0(tmp_path):
    assert lease_mod.main(["check", "--lease", str(tmp_path / "lease.json")]) == 0


def test_usage_error_exits_2():
    with pytest.raises(SystemExit) as exc:
        lease_mod.main(["acquire", "--table", TABLE])
    assert exc.value.code == 2


@pytest.fixture
def no_aws_client(monkeypatch):
    """boto3.client fails as it does with no AWS region configured."""
    from botocore.exceptions import NoRegionError

    def fail(*args, **kwargs):
        raise NoRegionError()

    monkeypatch.setattr(lease_mod.boto3, "client", fail)


def test_release_without_lease_file_needs_no_aws_env(tmp_path, no_aws_client, capsys):
    # The `when: always` cleanup after a failed acquire must pass whatever the AWS env.
    path = tmp_path / "lease.json"
    assert lease_mod.main(["release", "--table", TABLE, "--lease", str(path)]) == 0
    assert "nothing to release" in capsys.readouterr().out


def test_check_needs_no_aws_env(tmp_path, no_aws_client):
    path = tmp_path / "lease.json"
    assert lease_mod.main(["check", "--lease", str(path)]) == 0
    (tmp_path / "lease.lost").write_text("lost\n")
    assert lease_mod.main(["check", "--lease", str(path)]) == 4
