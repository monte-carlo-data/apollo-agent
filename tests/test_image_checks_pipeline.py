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


ODD_IMPACT_MAP = textwrap.dedent(
    """\
    import json
    print(json.dumps({"schema_version": 1, "packaging_changed": True, "image_targets": ["azure"],
        "changed_packages": ["pyodbc"], "unmapped_packages": [{"name": "x"}]}))
    """
)


def test_e2e_oddly_shaped_impact_fields_do_not_crash_setup(repo):
    _commit(repo, "scripts/impact_map.py", ODD_IMPACT_MAP)
    _commit(repo, "requirements.txt", "pyodbc==5.2.0\n")
    out, log = _run_setup(repo, {"CIRCLE_BRANCH": "feature"})
    assert _selected(out) == ["azure"]
    assert "unmapped packages: {'name': 'x'}" in log


def test_unexpected_selection_error_fails_open(tmp_path, monkeypatch):
    (tmp_path / "t.yml").write_text(TEMPLATE)
    monkeypatch.setenv("CIRCLE_BRANCH", "feature")
    monkeypatch.delenv("CIRCLE_TAG", raising=False)

    def boom(args):
        raise RuntimeError("unexpected")

    monkeypatch.setattr(setup, "select_branch", boom)
    out = tmp_path / "out.yml"
    assert setup.main(["--template", str(tmp_path / "t.yml"), "--out", str(out)]) == 0
    assert _selected(out.read_text()) == ALL


def test_fail_open_impact_map_shape_is_accepted():
    """C1 fail-open lists every connection type; manual_checks may have a null connection_type."""
    stdout = _impact(
        ALL,
        connection_types=[f"type-{i}" for i in range(38)],
        manual_checks=[
            {"area": "other", "connection_type": None, "reason": "unmapped"}
        ],
        unmapped_packages=["somepkg"],
    )
    selection = setup.select_targets(["requirements.txt"], lambda: (0, stdout, ""))
    assert selection.targets == ALL
    assert selection.reason == "impact map selection"
    setup.log_impact(selection.impact)  # must not raise


@pytest.mark.parametrize(
    "path", ["scripts/image_checks/check_image.py", "scripts/image_checks/in_image.py"]
)
def test_changed_checks_run_on_every_target(path):
    selection = setup.select_targets([path, "apollo/x.py"], _never_called)
    assert selection.targets == ALL
    assert "image checks changed" in selection.reason


def test_changed_checks_keep_the_impact_map_result():
    stdout = _impact(["lambda"])
    selection = setup.select_targets(
        ["scripts/image_checks/in_image.py", "requirements-lambda.txt"],
        lambda: (0, stdout, ""),
    )
    assert selection.targets == ALL
    assert selection.impact is not None and selection.packaging_files


# -- version steps: only v* release tags count -----------------------------------------


def _version_command(step):
    """The shell of a `generate-version-number-*` command in the continuation config."""
    lines = (ROOT / ".circleci" / "continue_config.yml").read_text().splitlines()
    start = lines.index(f"  {step}:")
    begin = next(
        i for i in range(start, len(lines)) if lines[i].strip() == "command: |"
    )
    body = []
    for line in lines[begin + 1 :]:
        if not line.startswith(" " * 10):
            break
        body.append(line[10:])
    return "\n".join(body).replace("<< pipeline.number >>", "4242")


@pytest.fixture
def tagged_repo(tmp_path):
    # A release tag on an older commit and a non-release tag (like a CI dry-run tag)
    # on HEAD: `git describe` without --match would return the newer, wrong one.
    _git(tmp_path, "init", "-q", "-b", "main")
    _git(tmp_path, "config", "user.email", "t@example.com")
    _git(tmp_path, "config", "user.name", "t")
    _git(tmp_path, "commit", "-q", "--allow-empty", "-m", "release")
    _git(tmp_path, "tag", "v1.14.7")
    _git(tmp_path, "commit", "-q", "--allow-empty", "-m", "later")
    _git(tmp_path, "tag", "setup-test-1")
    return tmp_path


def _run_version_step(repo, step):
    bash_env = repo / "bash_env"
    bash_env.write_text("")
    subprocess.run(
        ["bash", "-eo", "pipefail", "-c", _version_command(step)],
        cwd=repo,
        check=True,
        capture_output=True,
        env={**os.environ, "BASH_ENV": str(bash_env)},
    )
    return bash_env.read_text()


def test_dev_version_ignores_non_release_tags(tagged_repo):
    assert "NEXT_VERSION=1.14.8rc4242" in _run_version_step(
        tagged_repo, "generate-version-number-dev"
    )


def test_prod_version_ignores_non_release_tags(tagged_repo):
    assert "VERSION_TAG=1.14.7" in _run_version_step(
        tagged_repo, "generate-version-number-prod"
    )


def test_prod_version_on_a_release_tag(tagged_repo):
    _git(tagged_repo, "tag", "v1.14.8")
    assert "VERSION_TAG=1.14.8" in _run_version_step(
        tagged_repo, "generate-version-number-prod"
    )
