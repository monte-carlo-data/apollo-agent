"""Tests for scripts/golden_agent/precheck.py (pure version comparison, no network)."""

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "golden_agent" / "precheck.py"
if not SCRIPT.is_file():
    # The `tests` Docker stages copy tests/ but not scripts/; run-linter runs these.
    pytest.skip("scripts/golden_agent is not in this checkout", allow_module_level=True)

spec = importlib.util.spec_from_file_location("golden_agent_precheck", SCRIPT)
precheck = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = precheck
spec.loader.exec_module(precheck)


@pytest.mark.parametrize(
    "running, target",
    [
        ("1.14.11rc1702", "1.14.11rc1701"),  # rc numbers compare numerically...
        ("1.14.11rc100", "1.14.11rc99"),  # ...not as strings ("100" < "99")
        ("1.14.12rc5", "1.14.11rc9999"),  # base-version bump beats any older rc
        ("1.15.0rc1", "1.14.99rc1"),
        ("2.0.0rc1", "1.99.99rc99"),
        ("1.14.11", "1.14.11rc1702"),  # a release is newer than its own rcs
        ("1.14.11.1", "1.14.11"),
    ],
)
def test_superseded_when_running_is_strictly_newer(running, target):
    assert precheck.decide(running, target) == precheck.SUPERSEDED


@pytest.mark.parametrize(
    "running, target",
    [
        (
            "1.14.11rc1702",
            "1.14.11rc1702",
        ),  # equal: reruns must still upgrade and validate
        ("1.14.11", "1.14.11.0"),  # equal after padding
        ("1.14.11rc1701", "1.14.11rc1702"),  # normal upgrade
        ("1.14.11rc99", "1.14.11rc100"),
        ("1.14.11rc9999", "1.14.12rc5"),
        ("1.14.10", "1.14.11rc1"),  # dev target after a release
        ("1.14.11rc5", "1.14.11"),
    ],
)
def test_proceeds_when_running_is_not_newer(running, target):
    assert precheck.decide(running, target) == precheck.PROCEED


@pytest.mark.parametrize(
    "running",
    [
        "",
        "null",
        "local",
        "latest",
        "1.14",
        "1.14.x",
        "1.14.11-rc1",
        "v1.14.11",
        "1.14.11rc",
    ],
)
def test_unreadable_running_version_proceeds(running):
    assert precheck.decide(running, "1.14.11rc1") == precheck.PROCEED


def test_unparsable_target_proceeds():
    assert precheck.decide("1.14.12rc1", "${NEXT_VERSION}") == precheck.PROCEED


def test_surrounding_whitespace_is_ignored():
    assert precheck.decide(" 1.14.12rc1\n", "1.14.11rc1") == precheck.SUPERSEDED


def test_main_prints_marker_with_both_versions(capsys):
    assert (
        precheck.main(["--running", "1.14.11rc1702", "--target", "1.14.11rc1701"]) == 0
    )
    out = capsys.readouterr().out.splitlines()
    assert out[-1] == precheck.SUPERSEDED
    marker = [line for line in out if "golden-agent-superseded" in line]
    assert len(marker) == 1
    assert "1.14.11rc1702" in marker[0] and "1.14.11rc1701" in marker[0]


@pytest.mark.parametrize("running", ["1.14.11rc1701", "", "garbage"])
def test_main_proceed_has_no_marker(capsys, running):
    assert precheck.main(["--running", running, "--target", "1.14.11rc1701"]) == 0
    out = capsys.readouterr().out
    assert out.splitlines()[-1] == precheck.PROCEED
    assert "golden-agent-superseded" not in out


def _golden_instances() -> dict[str, list[str]]:
    """Each workflow instance of the golden job in continue_config.yml, by name, as its lines."""
    lines = (ROOT / ".circleci" / "continue_config.yml").read_text().splitlines()
    instances: dict[str, list[str]] = {}
    for i, line in enumerate(lines):
        if line.strip() != "- update-and-validate-golden-agent:":
            continue
        indent = len(line) - len(line.lstrip())
        block = []
        for following in lines[i + 1 :]:
            if following.strip() and len(following) - len(following.lstrip()) <= indent:
                break
            block.append(following.strip())
        name = next(b.split(":", 1)[1].strip() for b in block if b.startswith("name:"))
        instances[name] = block
    return instances


def test_only_dev_golden_jobs_skip_when_superseded():
    instances = _golden_instances()
    dev = {n for n in instances if "-dev-" in n}
    assert dev == {
        f"{p}-dev-update-and-validate-golden-agent" for p in ("gcp", "az", "aws")
    }
    assert len(instances) == 6
    for name, block in instances.items():
        assert ("skip_if_superseded: true" in block) == (name in dev), name
