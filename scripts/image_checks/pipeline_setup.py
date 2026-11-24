#!/usr/bin/env python3
"""Setup step of the CircleCI pipeline: decide which images a branch build checks.

Runs in the setup workflow (``.circleci/config.yml``) with a bare Python (stdlib
only, Python >= 3.9). It renders ``.circleci/continue_config.yml`` into the config
the pipeline continues with, keeping only the ``image-check-<target>`` jobs that
this build needs:

* tags, ``dev``, ``main`` and merge-queue branches: none. Those pipelines check
  every image they build inside their build jobs.
* other branches that change the checks (check_image.py, in_image.py): all four.
* other branches with no packaging change (requirements*, Dockerfile, .dockerignore,
  resources/): none, so code-only PRs build no images.
* other branches with a packaging change: the ``image_targets`` reported by
  ``scripts/impact_map.py`` (contract C1 of YET-3659). Fail-open: when the script is
  missing, exits non-zero or prints something unexpected, all four targets.

Markers in the template, for each target in TARGETS:
    # image-check:begin <target>   ... lines ...   # image-check:end <target>
    <a single line>  # image-check:<target>
Blocks and lines of targets that are not selected are removed.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

TARGETS: tuple[str, ...] = ("generic", "cloudrun", "lambda", "azure")
DEFAULT_IMPACT_MAP = "scripts/impact_map.py"
MAIN_BRANCH = "main"
UNCHECKED_BRANCHES = ("dev", "main")
QUEUE_PREFIX = "gh-readonly-queue/"
PACKAGING_PATTERNS = (
    re.compile(r"^requirements[^/]*$"),
    re.compile(r"^Dockerfile$"),
    re.compile(r"^\.dockerignore$"),
    re.compile(r"^resources/"),
)
# The checks themselves: a change to them is exercised on every target.
CHECK_FILES = (
    "scripts/image_checks/check_image.py",
    "scripts/image_checks/in_image.py",
)
IMPACT_MAP_TIMEOUT = 300

_BEGIN = re.compile(r"^\s*# image-check:begin (\S+)\s*$")
_END = re.compile(r"^\s*# image-check:end (\S+)\s*$")
_LINE = re.compile(r"#\s*image-check:(\S+)\s*$")


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


@dataclass
class Selection:
    targets: list[str]
    reason: str
    impact: dict[str, Any] | None = None
    packaging_files: list[str] = field(default_factory=list)


def is_packaging_file(path: str) -> bool:
    return any(p.search(path) for p in PACKAGING_PATTERNS)


def pipeline_kind(branch: str | None, tag: str | None) -> str:
    if tag:
        return "tag"
    if not branch or branch in UNCHECKED_BRANCHES or branch.startswith(QUEUE_PREFIX):
        return "protected"
    return "branch"


def validate_impact(impact: Any) -> list[str]:
    """The image targets of a C1 impact-map result; raises ValueError when it doesn't match C1."""
    if not isinstance(impact, dict) or impact.get("schema_version") != 1:
        raise ValueError("schema_version is not 1")
    targets = impact.get("image_targets")
    if not isinstance(targets, list) or not all(isinstance(t, str) for t in targets):
        raise ValueError("image_targets is not a list of strings")
    unknown = sorted(set(targets) - set(TARGETS))
    if unknown:
        raise ValueError(f"unknown image_targets {unknown}")
    if not isinstance(impact.get("packaging_changed"), bool):
        raise ValueError("packaging_changed is not a boolean")
    return [t for t in TARGETS if t in targets]


ImpactRunner = Callable[[], "tuple[int, str, str] | None"]


def select_targets(
    changed_files: list[str] | None, run_impact_map: ImpactRunner
) -> Selection:
    """``run_impact_map`` returns (exit code, stdout, stderr), or None when the script is absent."""
    if changed_files is None:
        return Selection(
            list(TARGETS),
            "could not list the changed files; checking every target (fail-open)",
        )
    checks = [f for f in changed_files if f in CHECK_FILES]
    if checks:
        return Selection(
            list(TARGETS),
            f"the image checks changed ({', '.join(checks)}); checking every target",
        )
    packaging = [f for f in changed_files if is_packaging_file(f)]
    if not packaging:
        return Selection([], "no packaging change; no image is built")
    result = run_impact_map()
    if result is None:
        return Selection(
            list(TARGETS),
            f"{DEFAULT_IMPACT_MAP} not found; checking every target (fail-open)",
            packaging_files=packaging,
        )
    code, stdout, stderr = result
    if code != 0:
        detail = "\n".join(stderr.strip().splitlines()[-5:])
        return Selection(
            list(TARGETS),
            f"impact map exited {code}; checking every target (fail-open)\n{detail}".rstrip(),
            packaging_files=packaging,
        )
    try:
        impact = json.loads(stdout)
        targets = validate_impact(impact)
    except ValueError as exc:  # json.JSONDecodeError is a ValueError
        return Selection(
            list(TARGETS),
            f"impact map output not understood ({exc}); checking every target (fail-open)",
            packaging_files=packaging,
        )
    if not targets:
        return Selection([], "impact map: no image affected", impact, packaging)
    return Selection(targets, "impact map selection", impact, packaging)


def render(template: str, targets: list[str]) -> str:
    """Drop the blocks and lines of targets not in ``targets``; raises ValueError on bad markers."""
    selected = set(targets)
    seen: set[str] = set()
    out: list[str] = []
    open_block: str | None = None
    for number, line in enumerate(template.splitlines(keepends=True), start=1):
        begin, end = _BEGIN.match(line), _END.match(line)
        if begin:
            if open_block:
                raise ValueError(
                    f"line {number}: block {begin.group(1)} opened inside {open_block}"
                )
            name = begin.group(1)
            _known(name, number)
            seen.add(name)
            open_block = name
            continue
        if end:
            if end.group(1) != open_block:
                raise ValueError(
                    f"line {number}: end {end.group(1)} does not close {open_block}"
                )
            open_block = None
            continue
        if open_block:
            if open_block in selected:
                out.append(line)
            continue
        tagged = _LINE.search(line)
        if tagged:
            _known(tagged.group(1), number)
            seen.add(tagged.group(1))
            if tagged.group(1) not in selected:
                continue
        out.append(line)
    if open_block:
        raise ValueError(f"block {open_block} is never closed")
    missing = sorted(set(TARGETS) - seen)
    if missing:
        raise ValueError(f"template has no markers for {missing}")
    return "".join(out)


def _known(target: str, number: int) -> None:
    if target not in TARGETS:
        raise ValueError(f"line {number}: unknown target {target!r}")


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], check=True, capture_output=True, text=True, timeout=120
    ).stdout


def changed_files_against_main() -> tuple[str, list[str] | None]:
    """(merge base with main, files changed since it); ("", None) when git fails."""
    try:
        # The setup job's checkout may not have main; a failure here falls back to fail-open.
        git(
            "fetch",
            "--quiet",
            "origin",
            f"+refs/heads/{MAIN_BRANCH}:refs/remotes/origin/{MAIN_BRANCH}",
        )
        base = git("merge-base", f"origin/{MAIN_BRANCH}", "HEAD").strip()
        return base, [
            f for f in git("diff", "--name-only", base, "HEAD").splitlines() if f
        ]
    except (subprocess.SubprocessError, OSError) as exc:
        log(f"could not diff against origin/{MAIN_BRANCH}: {exc}")
        return "", None


def impact_map_runner(script: Path, base: str, fmt: str = "json") -> ImpactRunner:
    def run() -> tuple[int, str, str] | None:
        if not script.is_file():
            return None
        try:
            proc = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--base",
                    base,
                    "--head",
                    "HEAD",
                    "--format",
                    fmt,
                ],
                capture_output=True,
                text=True,
                timeout=IMPACT_MAP_TIMEOUT,
            )
        except (subprocess.SubprocessError, OSError) as exc:
            return 1, "", str(exc)
        return proc.returncode, proc.stdout, proc.stderr

    return run


def write_checklist(script: Path, base: str, path: Path) -> None:
    """Best effort: the impact map's markdown checklist for the PR comment (empty if none)."""
    result = impact_map_runner(script, base, "markdown")()
    text = result[1] if result and result[0] == 0 else ""
    path.write_text(text)
    log(
        f"manual checklist: {'%d lines' % len(text.splitlines()) if text.strip() else 'none'}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--template", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument(
        "--checklist", type=Path, help="write the impact map's markdown checklist here"
    )
    parser.add_argument("--impact-map", type=Path, default=Path(DEFAULT_IMPACT_MAP))
    parser.add_argument(
        "--targets", help="override: comma-separated targets, skipping selection"
    )
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    kind = pipeline_kind(os.environ.get("CIRCLE_BRANCH"), os.environ.get("CIRCLE_TAG"))
    if args.targets is not None:
        targets = [t for t in args.targets.split(",") if t]
        selection = Selection(
            [t for t in TARGETS if t in targets], "--targets override"
        )
    elif kind != "branch":
        selection = Selection(
            [], f"{kind} build: its build jobs check every image they build"
        )
    else:
        try:
            selection = select_branch(args)
        except Exception as exc:  # a setup failure would stop the whole pipeline
            selection = Selection(
                list(TARGETS),
                f"selection failed ({type(exc).__name__}: {exc}); checking every target (fail-open)",
            )
    log(f"image checks: {', '.join(selection.targets) or 'none'} ({selection.reason})")
    args.out.write_text(render(args.template.read_text(), selection.targets))
    return 0


def select_branch(args: argparse.Namespace) -> Selection:
    base, changed = changed_files_against_main()
    selection = select_targets(changed, impact_map_runner(args.impact_map, base))
    if args.checklist and selection.impact is not None:
        write_checklist(args.impact_map, base, args.checklist)
    elif args.checklist and changed is not None and not selection.packaging_files:
        # Definitely nothing to check; clears an earlier checklist. When the impact map
        # is missing or failed, no file is written and the comment is left alone.
        args.checklist.write_text("")
    if selection.packaging_files:
        log(f"packaging files changed: {', '.join(selection.packaging_files)}")
    log_impact(selection.impact)
    return selection


def log_impact(impact: dict[str, Any] | None) -> None:
    """Log the package changes; fields beyond image_targets aren't validated, so str() them."""
    if not impact:
        return
    packages = impact.get("changed_packages")
    for pkg in packages if isinstance(packages, list) else []:
        if isinstance(pkg, dict):
            log(f"  {pkg.get('name')}: {pkg.get('from')} -> {pkg.get('to')}")
    unmapped = impact.get("unmapped_packages")
    if isinstance(unmapped, list) and unmapped:
        log(f"  unmapped packages: {', '.join(str(p) for p in unmapped)}")


if __name__ == "__main__":
    sys.exit(main())
