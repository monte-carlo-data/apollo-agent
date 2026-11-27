#!/usr/bin/env python3
"""Decide whether a dev golden-agent job is superseded (YET-3767).

    precheck.py --running <agent's running version> --target <version this job installs>

Prints `superseded` or `proceed` as its last stdout line and always exits 0; the job
reads that line. `superseded` means the agent already runs a version strictly newer than
the target: a newer `dev` pipeline upgraded it first, and installing the target would be
a downgrade the agent's version floor rejects. It is preceded by one line with the
marker `golden-agent-superseded` and both versions.

Anything else proceeds, so a real problem still fails loudly in the upgrade: an equal
version (a rerun), an older one, or a version that can't be parsed (agent unreachable,
"local", empty output). Versions are compared as `X.Y.Z[.N...]` plus an optional
`a`/`b`/`rc` pre-release number, numerically (PEP 440 order for the shapes the agent
uses: rc1701 < rc1702, 1.14.11rc9999 < 1.14.12rc5, 1.14.11rc5 < 1.14.11). Stdlib only.
"""

from __future__ import annotations

import argparse
import re
import sys

SUPERSEDED = "superseded"
PROCEED = "proceed"
MARKER = "golden-agent-superseded"

_VERSION = re.compile(r"(\d+(?:\.\d+){2,})(?:(a|b|rc)(\d+))?")
# A final release sorts after all of its pre-releases.
_PRE_RANK = {"a": 0, "b": 1, "rc": 2, None: 3}


def parse(version: str) -> tuple[tuple[int, ...], int, int] | None:
    match = _VERSION.fullmatch(version.strip())
    if not match:
        return None
    release = [int(part) for part in match.group(1).split(".")]
    while len(release) > 3 and release[-1] == 0:
        release.pop()  # 1.14.11.0 == 1.14.11
    pre = match.group(2)
    return tuple(release), _PRE_RANK[pre], int(match.group(3) or 0)


def decide(running: str, target: str) -> str:
    running_key, target_key = parse(running), parse(target)
    if running_key is None or target_key is None:
        return PROCEED
    return SUPERSEDED if running_key > target_key else PROCEED


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Decide whether a dev golden-agent job is superseded."
    )
    parser.add_argument("--running", required=True, help="the agent's running version")
    parser.add_argument("--target", required=True, help="the version this job installs")
    args = parser.parse_args(argv)
    running, target = args.running.strip(), args.target.strip()
    decision = decide(running, target)
    if decision == SUPERSEDED:
        print(
            f"{MARKER}: the agent already runs {running}, newer than this job's target"
            f" {target}; a newer dev pipeline upgraded it, so this job skips the upgrade"
            " and validations and ends successfully"
        )
    elif parse(running) is None:
        print(
            f"Could not parse the agent's running version ({running!r}); upgrading anyway"
        )
    else:
        print(f"Agent runs {running}, target {target}: upgrading")
    print(decision)
    return 0


if __name__ == "__main__":
    sys.exit(main())
