"""Tests for scripts/image_checks/pipeline_setup.py and pr_comment.py (CI setup logic)."""

import importlib.util
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts" / "image_checks"
if not SCRIPTS.is_dir():
    # The `tests` Docker stages copy tests/ but not scripts/; run-linter runs these.
    pytest.skip("scripts/image_checks is not in this checkout", allow_module_level=True)


def _load(name):
    spec = importlib.util.spec_from_file_location(
        f"image_checks_{name}", SCRIPTS / f"{name}.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


setup = _load("pipeline_setup")
pr_comment = _load("pr_comment")

ALL = ["generic", "cloudrun", "lambda", "azure"]


def _impact(targets, packaging_changed=True, **extra):
    return json.dumps(
        {
            "schema_version": 1,
            "packaging_changed": packaging_changed,
            "changed_files": [],
            "changed_packages": [],
            "image_targets": targets,
            "platforms": [],
            "connection_types": [],
            "manual_checks": [],
            "unmapped_packages": [],
            **extra,
        }
    )


def _never_called():
    raise AssertionError("impact map should not run")


# -- packaging filter and pipeline kind ------------------------------------------------


@pytest.mark.parametrize(
    "path",
    [
        "requirements.txt",
        "requirements-lambda.in",
        "requirements-dev.txt",
        "Dockerfile",
        ".dockerignore",
        "resources/lambda/openssl/openssl_tls1.cnf",
    ],
)
def test_packaging_files(path):
    assert setup.is_packaging_file(path)


@pytest.mark.parametrize(
    "path",
    [
        "apollo/agent/agent.py",
        "tests/requirements.txt",
        "docs/Dockerfile.md",
        "scripts/image_checks/check_image.py",
        ".circleci/config.yml",
    ],
)
def test_non_packaging_files(path):
    assert not setup.is_packaging_file(path)


@pytest.mark.parametrize(
    "branch,tag,kind",
    [
        ("feature/x", None, "branch"),
        ("dev", None, "protected"),
        ("main", None, "protected"),
        ("gh-readonly-queue/main/pr-12-abc", None, "protected"),
        (None, "v1.2.3", "tag"),
        ("", "v1.2.3", "tag"),
        (None, None, "protected"),
    ],
)
def test_pipeline_kind(branch, tag, kind):
    assert setup.pipeline_kind(branch, tag) == kind


# -- target selection ------------------------------------------------------------------


def test_code_only_change_selects_nothing_and_skips_impact_map():
    selection = setup.select_targets(["apollo/agent/agent.py"], _never_called)
    assert selection.targets == []


def test_diff_failure_fails_open():
    assert setup.select_targets(None, _never_called).targets == ALL


def test_absent_impact_map_fails_open():
    selection = setup.select_targets(["requirements.txt"], lambda: None)
    assert selection.targets == ALL
    assert "fail-open" in selection.reason


def test_impact_map_error_fails_open():
    selection = setup.select_targets(
        ["requirements.txt"], lambda: (2, "", "Traceback\nboom")
    )
    assert selection.targets == ALL
    assert "exited 2" in selection.reason and "boom" in selection.reason


@pytest.mark.parametrize(
    "stdout",
    [
        "not json",
        json.dumps(
            {"schema_version": 2, "image_targets": [], "packaging_changed": True}
        ),
        _impact(["generic", "aws_proxied"]),
        _impact("generic"),
        json.dumps({"schema_version": 1, "image_targets": ["generic"]}),
    ],
)
def test_unexpected_impact_map_output_fails_open(stdout):
    assert (
        setup.select_targets(["requirements.txt"], lambda: (0, stdout, "")).targets
        == ALL
    )


def test_impact_map_selection_in_canonical_order():
    selection = setup.select_targets(
        ["requirements-lambda.txt"], lambda: (0, _impact(["azure", "lambda"]), "")
    )
    assert selection.targets == ["lambda", "azure"]
    assert selection.impact["schema_version"] == 1


def test_impact_map_no_targets_selects_nothing():
    stdout = _impact([], packaging_changed=False)
    assert (
        setup.select_targets(["requirements-dev.txt"], lambda: (0, stdout, "")).targets
        == []
    )


# -- rendering -------------------------------------------------------------------------

TEMPLATE = textwrap.dedent(
    """\
    jobs:
      # image-check:begin generic
      - image-check:
          name: image-check-generic
      # image-check:end generic
      # image-check:begin cloudrun
      - image-check-cloudrun
      # image-check:end cloudrun
      # image-check:begin lambda
      - image-check-lambda
      # image-check:end lambda
      # image-check:begin azure
      - image-check-azure
      # image-check:end azure
      - build-ok:
          requires:
            - run-linter
            - image-check-generic  # image-check:generic
            - image-check-cloudrun  # image-check:cloudrun
            - image-check-lambda  # image-check:lambda
            - image-check-azure  # image-check:azure
    """
)


def test_render_keeps_only_selected_targets():
    out = setup.render(TEMPLATE, ["lambda"])
    assert "image-check-lambda" in out
    for other in ("generic", "cloudrun", "azure"):
        assert f"image-check-{other}" not in out
    assert "run-linter" in out and "image-check:begin" not in out


def test_render_nothing_selected():
    out = setup.render(TEMPLATE, [])
    assert "image-check-" not in out
    assert out.endswith("- run-linter\n")


@pytest.mark.parametrize(
    "template,error",
    [
        (TEMPLATE.replace("begin azure", "begin nope"), "unknown target"),
        (TEMPLATE.replace("# image-check:end lambda\n", ""), "opened inside"),
        (TEMPLATE.replace("# image-check:begin generic\n", ""), "does not close"),
        (
            TEMPLATE.replace("# image-check:azure", "# image-check:azur"),
            "unknown target",
        ),
        ("jobs: []\n", "no markers"),
    ],
)
def test_render_rejects_bad_markers(template, error):
    with pytest.raises(ValueError, match=error):
        setup.render(template, ALL)


@pytest.mark.parametrize("targets", [[], ["generic"], ["cloudrun", "azure"], ALL])
def test_render_real_continuation_config(targets):
    template = (ROOT / ".circleci" / "continue_config.yml").read_text()
    out = setup.render(template, targets)
    for target in ALL:
        name = f"name: image-check-{target}"
        assert (name in out) == (target in targets)
        assert (f"- image-check-{target}  #" in out) == (target in targets)
    # the required checks are always there
    for job in (
        "build-ok:",
        "run-linter",
        "run-test-docker:",
        "run-test-docker-azure:",
    ):
        assert job in out


# -- end to end in a throwaway git repo ------------------------------------------------


def _git(cwd, *args):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path):
    """A clone of a repo whose main has requirements.txt, on branch `feature`."""
    origin = tmp_path / "origin"
    origin.mkdir()
    _git(origin, "init", "-q", "-b", "main")
    _git(origin, "config", "user.email", "t@example.com")
    _git(origin, "config", "user.name", "t")
    (origin / "requirements.txt").write_text("pyodbc==5.1.0\n")
    (origin / "app.py").write_text("x = 1\n")
    _git(origin, "add", ".")
    _git(origin, "commit", "-q", "-m", "init")
    clone = tmp_path / "clone"
    _git(tmp_path, "clone", "-q", str(origin), str(clone))
    _git(clone, "config", "user.email", "t@example.com")
    _git(clone, "config", "user.name", "t")
    _git(clone, "checkout", "-q", "-b", "feature")
    (clone / "template.yml").write_text(TEMPLATE)
    return clone


def _commit(repo, path, text):
    (repo / path).parent.mkdir(parents=True, exist_ok=True)
    (repo / path).write_text(text)
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", f"change {path}")


def _run_setup(repo, env_overrides):
    env = {k: v for k, v in os.environ.items() if not k.startswith("CIRCLE_")}
    env.update(env_overrides)
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPTS / "pipeline_setup.py"),
            "--template",
            "template.yml",
            "--out",
            "out.yml",
            "--checklist",
            "checklist.md",
        ],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    return (repo / "out.yml").read_text(), proc.stderr


def _selected(out):
    return [t for t in ALL if f"image-check-{t}  #" in out]


def test_e2e_code_only_branch(repo):
    _commit(repo, "app.py", "x = 2\n")
    out, log = _run_setup(repo, {"CIRCLE_BRANCH": "feature"})
    assert _selected(out) == []
    assert "no packaging change" in log
    assert (repo / "checklist.md").read_text() == ""


def test_e2e_packaging_change_without_impact_map_fails_open(repo):
    _commit(repo, "requirements.txt", "pyodbc==5.2.0\n")
    out, log = _run_setup(repo, {"CIRCLE_BRANCH": "feature"})
    assert _selected(out) == ALL
    assert "not found" in log and "fail-open" in log
    # no checklist file: an earlier PR comment is left alone
    assert not (repo / "checklist.md").exists()


STUB_IMPACT_MAP = textwrap.dedent(
    """\
    import argparse, json
    p = argparse.ArgumentParser()
    p.add_argument("--base"); p.add_argument("--head"); p.add_argument("--format", default="json")
    a = p.parse_args()
    if a.format == "markdown":
        print("- [ ] sql-server: run a metadata collection")
    else:
        print(json.dumps({"schema_version": 1, "packaging_changed": True,
            "changed_files": ["requirements.txt"],
            "changed_packages": [{"name": "pyodbc", "from": "5.1.0", "to": "5.2.0",
                                  "requirements_files": ["requirements.txt"]}],
            "image_targets": ["lambda", "generic"], "platforms": ["aws"],
            "connection_types": ["sql-server"], "manual_checks": [], "unmapped_packages": []}))
    """
)


def test_e2e_packaging_change_with_impact_map(repo):
    _commit(repo, "scripts/impact_map.py", STUB_IMPACT_MAP)
    _commit(repo, "requirements.txt", "pyodbc==5.2.0\n")
    out, log = _run_setup(repo, {"CIRCLE_BRANCH": "feature"})
    assert _selected(out) == ["generic", "lambda"]
    assert "pyodbc: 5.1.0 -> 5.2.0" in log
    assert "sql-server" in (repo / "checklist.md").read_text()


def test_e2e_crashing_impact_map_fails_open(repo):
    _commit(repo, "scripts/impact_map.py", "raise SystemExit(3)\n")
    _commit(repo, "requirements.txt", "pyodbc==5.2.0\n")
    out, log = _run_setup(repo, {"CIRCLE_BRANCH": "feature"})
    assert _selected(out) == ALL
    assert "exited 3" in log


@pytest.mark.parametrize(
    "env",
    [
        {"CIRCLE_TAG": "v1.2.3"},
        {"CIRCLE_BRANCH": "gh-readonly-queue/main/pr-1-abc"},
        {"CIRCLE_BRANCH": "dev"},
        {"CIRCLE_BRANCH": "main"},
    ],
)
def test_e2e_protected_builds_get_no_branch_checks(repo, env):
    _commit(repo, "requirements.txt", "pyodbc==5.2.0\n")
    out, log = _run_setup(repo, env)
    assert _selected(out) == []
    assert "build jobs check every image" in log


# -- PR comment ------------------------------------------------------------------------


def test_pr_number():
    assert pr_comment.pr_number("https://github.com/o/r/pull/512") == 512
    assert pr_comment.pr_number(None) is None
    assert pr_comment.pr_number("https://github.com/o/r/tree/x") is None


def test_comment_skipped_without_token(tmp_path):
    body = tmp_path / "c.md"
    body.write_text("- [ ] check")
    assert "IMPACT_MAP_GITHUB_TOKEN is not set" in pr_comment.run(body, {})


def test_comment_skipped_outside_pr(tmp_path):
    body = tmp_path / "c.md"
    body.write_text("- [ ] check")
    assert "not a pull request" in pr_comment.run(
        body, {"IMPACT_MAP_GITHUB_TOKEN": "x"}
    )


def test_comment_skipped_without_checklist_file(tmp_path):
    env = {
        "IMPACT_MAP_GITHUB_TOKEN": "x",
        "CIRCLE_PULL_REQUEST": "https://github.com/o/r/pull/1",
    }
    assert "no checklist" in pr_comment.run(tmp_path / "missing.md", env)


class FakeGitHub(pr_comment.GitHub):
    def __init__(self, comments):
        super().__init__("token", "o/r")
        self.comments = comments
        self.calls = []

    def request(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        return self.comments if method == "GET" else {}


def test_upsert_posts_then_updates():
    gh = FakeGitHub([{"id": 1, "body": "unrelated"}])
    assert gh.upsert(7, "- [ ] a") == "posted the checklist"
    assert gh.calls[-1][0] == "POST" and pr_comment.MARKER in gh.calls[-1][2]["body"]

    gh = FakeGitHub([{"id": 9, "body": f"{pr_comment.MARKER}\nold"}])
    assert gh.upsert(7, "") == "updated the checklist"
    method, path, payload = gh.calls[-1]
    assert (method, path) == ("PATCH", "/repos/o/r/issues/comments/9")
    assert pr_comment.EMPTY_BODY in payload["body"]


def test_upsert_does_not_post_empty_checklist():
    gh = FakeGitHub([])
    assert "not posting" in gh.upsert(7, "  \n")
    assert [c[0] for c in gh.calls] == ["GET"]


def test_main_never_fails(tmp_path, monkeypatch, capsys):
    body = tmp_path / "c.md"
    body.write_text("- [ ] a")
    monkeypatch.setenv("IMPACT_MAP_GITHUB_TOKEN", "x")
    monkeypatch.setenv("CIRCLE_PULL_REQUEST", "https://github.com/o/r/pull/1")

    def boom(self, number):
        raise ConnectionError("network down")

    monkeypatch.setattr(pr_comment.GitHub, "find_comment", boom)
    assert pr_comment.main(["--body-file", str(body)]) == 0
    assert "network down; ignored" in capsys.readouterr().out
