"""Where dev and merge-queue builds push their images (.circleci/continue_config.yml).

dev pushes to pre-release-agent / mcd-pre-release-agent with latest-* tags; the merge queue
pushes to queue-agent / mcd-queue-agent with versioned tags only (YET-3661). Stdlib only:
run-linter runs these in a bare venv with pytest alone, so the config is read as text.
"""

import os
import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CONFIG = (ROOT / ".circleci" / "continue_config.yml").read_text()
QUEUE_FILTER = "/^gh-readonly-queue\\/.*/"


def _section(text, start, end):
    begin = text.index(start)
    return text[begin : text.index(end, begin + len(start))]


def _definition(name):
    """A job or command definition: from `  <name>:` to the next 2-space key or comment."""
    match = re.search(rf"(?ms)^  {re.escape(name)}:\n(.*?)(?=^  [a-z#]|^\S)", CONFIG)
    assert match, name
    return match.group(1)


def _workflow_entries():
    """{name: entry text} for the build-and-deploy workflow's job entries."""
    workflow = CONFIG[CONFIG.index("\nworkflows:\n") :]
    entries = {}
    for chunk in re.split(r"(?m)^      - ", workflow)[1:]:
        match = re.search(r"(?m)^          name: (\S+)$", chunk)
        if match:
            entries[match.group(1)] = chunk
    return entries


def _branches_only(entry):
    match = re.search(r"branches:\n\s+only:\n((?:\s+- .+\n)+)", entry)
    return [line.strip()[2:] for line in match.group(1).splitlines()] if match else []


ENTRIES = _workflow_entries()


def _queue_entries():
    return {n: e for n, e in ENTRIES.items() if QUEUE_FILTER in _branches_only(e)}


def _dev_entries():
    return {n: e for n, e in ENTRIES.items() if "dev" in _branches_only(e)}


def test_no_job_runs_on_both_dev_and_the_queue():
    both = set(_queue_entries()) & set(_dev_entries())
    assert not both


def test_queue_jobs_use_only_the_queue_repos():
    queue = _queue_entries()
    expected = {
        "build-and-push-queue-azure",
        "build-and-push-queue-cloudrun",
        "build-and-push-queue-aws-proxied",
        "build-and-push-lambda-queue",
        "push-lambda-aws-queue",
        "gcp-prod-update-and-validate-golden-agent-merge-queue",
        "az-prod-update-and-validate-golden-agent-merge-queue",
        "aws-prod-update-and-validate-golden-agent-merge-queue",
    }
    assert expected <= set(queue), sorted(expected - set(queue))
    for name, entry in queue.items():
        assert "pre-release-agent" not in entry, name
        assert "${AWS_AGENT_ECR_REPO_DEV}" not in entry, name
        if "push_latest" in entry or re.search(
            r"docker_hub_repository|aws_ecr_repository", entry
        ):
            assert "push_latest: false" in entry, name
        if re.search(r"docker_hub_repository|image_repo|image: montecarlodata/", entry):
            assert "queue-agent" in entry, name
        if "aws_ecr_repository" in entry or "ecr_repo_name" in entry:
            assert "mcd-queue-agent" in entry, name


def test_dev_jobs_keep_the_pre_release_repos_and_latest_tags():
    dev = _dev_entries()
    assert dev
    for name, entry in dev.items():
        assert "queue-agent" not in entry, name
        assert "push_latest: false" not in entry, name


def test_merge_queue_gate_requires_the_queue_builds():
    build_ok = _section(CONFIG, "      - build-ok:", "filters:")
    required = re.findall(r"- (\S+)", build_ok)
    for name in required:
        if name in ENTRIES:
            assert "-dev-" not in name and not name.endswith("-dev"), name
    assert "build-and-push-queue-aws-proxied" in required
    for golden in ("gcp", "az", "aws"):
        name = f"{golden}-prod-update-and-validate-golden-agent-merge-queue"
        assert name in required
        needs = re.findall(
            r"(?m)^            - (\S+)$",
            _section(ENTRIES[name], "requires:", "filters:"),
        )
        assert needs and all(n in _queue_entries() for n in needs), (name, needs)


# -- the push commands, run with a fake docker ------------------------------------------


def _job_command(job, step_name):
    """The shell of a job's `run` step, with << parameters.* >> left in."""
    body = _definition(job)
    step = body.index(f"name: {step_name}")
    lines = body[step:].splitlines()[1:]
    assert lines[0].strip() == "command: |"
    indent = len(lines[1]) - len(lines[1].lstrip())
    out = []
    for line in lines[1:]:
        if line.strip() and not line.startswith(" " * indent):
            break
        out.append(line[indent:])
    return "\n".join(out)


def _run(command, params, tmp_path):
    for key, value in params.items():
        command = command.replace(f"<< parameters.{key} >>", value)
    command = command.replace("<< pipeline.number >>", "7")
    assert "<<" not in command
    fake = tmp_path / "bin"
    fake.mkdir()
    (fake / "docker").write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$DOCKER_ARGS"\n')
    (fake / "docker").chmod(0o755)
    args = tmp_path / "args"
    env = {
        **os.environ,
        "PATH": f"{fake}:{os.environ['PATH']}",
        "DOCKER_ARGS": str(args),
    }
    subprocess.run(
        ["bash", "-eo", "pipefail", "-c", command],
        check=True,
        env=env,
        capture_output=True,
    )
    words = args.read_text().split()
    return sorted(words[i + 1] for i, w in enumerate(words) if w == "--tag")


IMAGE_STEP = "Build and push << parameters.target >> with SBOM + SLSA provenance"


@pytest.mark.parametrize(
    "push_latest, alias, expected",
    [
        ("true", "", ["r:1.2.3rc7-azure", "r:latest-azure"]),
        ("false", "", ["r:1.2.3rc7-azure"]),
        (
            "true",
            "aws-generic",
            [
                "r:1.2.3rc7-aws-generic",
                "r:1.2.3rc7-azure",
                "r:latest-aws-generic",
                "r:latest-azure",
            ],
        ),
        ("false", "aws-generic", ["r:1.2.3rc7-aws-generic", "r:1.2.3rc7-azure"]),
    ],
)
def test_image_push_tags(tmp_path, push_latest, alias, expected):
    command = _job_command("build-and-push-image", IMAGE_STEP)
    tags = _run(
        command,
        {
            "docker_hub_repository": "repo",
            "code_version": "1.2.3rc7",
            "target": "azure",
            "tag_suffix": "azure",
            "alias_tag_suffix": alias,
            "push_latest": push_latest,
        },
        tmp_path,
    )
    assert tags == [t.replace("r:", "montecarlodata/repo:") for t in expected]


@pytest.mark.parametrize(
    "push_latest, expected",
    [("true", ["1.2.3rc7-lambda", "latest-lambda"]), ("false", ["1.2.3rc7-lambda"])],
)
def test_lambda_docker_hub_push_tags(tmp_path, push_latest, expected):
    command = _job_command(
        "build-and-push-lambda",
        "Build and push lambda (Docker Hub) with SBOM + SLSA provenance",
    )
    tags = _run(
        command,
        {
            "docker_hub_repository": "repo",
            "code_version": "1.2.3rc7",
            "push_latest": push_latest,
        },
        tmp_path,
    )
    assert tags == [f"montecarlodata/repo:{t}" for t in expected]


@pytest.mark.parametrize("command", ["build-lambda-ecr", "push-lambda-ecr"])
def test_ecr_tags_follow_push_latest(command):
    body = _definition(command)
    when = _section(body, "      - when:", "      - unless:")
    unless = body[body.index("      - unless:") :]
    assert "condition: << parameters.push_latest >>" in when
    assert "condition: << parameters.push_latest >>" in unless
    assert "tag: latest,<< parameters.code_version >>" in when
    assert re.search(r"tag: << parameters.code_version >>$", unless.rstrip())
    assert "tag: latest" not in unless
