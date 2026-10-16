#!/usr/bin/env python3
"""verify_supply_chain.py — deterministic, tool-produced verification of a rybot
vuln-fix PR's branch. This is the OBJECTIVE check the auto-merge gate keys on so
mechanical dependency bumps can merge WITHOUT relying on the LLM reviewer's
opinion (which false-positived a real package on #152).

It does not trust the coding agent's prose. It re-runs the checks itself against
the checked-out fixed tree and emits a machine verdict.

Checks (per ecosystem detected in CWD):
  npm  (package-lock.json present):
    - `npm ci --ignore-scripts`          install resolves against the real
                                          registry (proves every pinned package,
                                          incl. new/overridden ones, EXISTS and
                                          its integrity hash matches) with no
                                          install scripts executed;
    - `lockfile-lint`                     every resolved URL is the real registry,
                                          https, and carries an integrity hash;
    - `npm audit --omit=dev`              no remaining high/critical advisories.
  python (uv.lock / requirements*.txt):
    - `uv lock --locked` (if uv.lock)     lockfile is internally consistent;
    - `pip-audit`                         no known vulns in the resolved set.

Fail-closed: an unknown ecosystem, a Dockerfile-only change (needs an image
build to verify — out of scope for this static check), or any check failure ->
ok=false. A green verdict means the mechanical fix is objectively safe to
auto-merge; a red/uncertain one holds the PR for a human.

Usage (from the repo root of the checked-out PR branch):
    python3 verify_supply_chain.py [--dir .] > verdict.json
Exit code 0 iff ok=true.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys


def _run(cmd: list[str], cwd: str, timeout: int = 600) -> tuple[int, str]:
    try:
        p = subprocess.run(
            cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout
        )
        return p.returncode, (p.stdout or "") + (p.stderr or "")
    except FileNotFoundError as e:
        return 127, f"not found: {e}"
    except subprocess.TimeoutExpired:
        return 124, f"timeout after {timeout}s: {' '.join(cmd)}"
    except Exception as e:  # pragma: no cover
        return 1, str(e)


def _tail(text: str, n: int = 1500) -> str:
    return text[-n:]


def verify_npm(d: str) -> dict:
    checks: list[dict] = []

    rc, out = _run(["npm", "ci", "--ignore-scripts"], d)
    checks.append({"name": "npm ci --ignore-scripts", "ok": rc == 0, "detail": _tail(out)})

    rc, out = _run(
        ["npx", "--yes", "lockfile-lint", "--path", "package-lock.json",
         "--type", "npm", "--validate-https", "--validate-integrity",
         "--allowed-hosts", "npm"],
        d,
    )
    checks.append({"name": "lockfile-lint", "ok": rc == 0, "detail": _tail(out)})

    # `npm audit` exits non-zero if advisories >= the level remain. We want the
    # post-fix tree clean of high/critical (deps PRs are supposed to CLEAR these).
    rc, out = _run(["npm", "audit", "--omit=dev", "--audit-level=high"], d)
    checks.append({"name": "npm audit (high+)", "ok": rc == 0, "detail": _tail(out)})

    return {"ecosystem": "npm", "ok": all(c["ok"] for c in checks), "checks": checks}


def verify_python(d: str) -> dict:
    checks: list[dict] = []

    if os.path.isfile(os.path.join(d, "uv.lock")):
        rc, out = _run(["uv", "lock", "--locked"], d)
        checks.append({"name": "uv lock --locked", "ok": rc == 0, "detail": _tail(out)})

    rc, out = _run(["python3", "-m", "pip", "install", "--quiet", "pip-audit"], d)
    if rc == 0:
        rc, out = _run(["python3", "-m", "pip_audit", "--strict"], d)
        checks.append({"name": "pip-audit", "ok": rc == 0, "detail": _tail(out)})
    else:
        checks.append({"name": "pip-audit (install)", "ok": False, "detail": _tail(out)})

    return {"ecosystem": "python", "ok": bool(checks) and all(c["ok"] for c in checks), "checks": checks}


_SKIP_DIRS = {"node_modules", ".git", "vendor", "dist", "build", ".venv", "venv"}
_PY_MARKERS = ("uv.lock", "requirements.txt", "pyproject.toml", "poetry.lock")


def _find_ecosystem_dirs(root: str) -> tuple[list[str], list[str]]:
    """Walk the tree (repos put lockfiles in subdirs — admin/, console/, …) and
    collect dirs that contain an npm lockfile and dirs that contain a python
    manifest/lock. Skips node_modules/.git/etc."""
    npm_dirs: list[str] = []
    py_dirs: list[str] = []
    for cur, dirs, files in os.walk(root):
        dirs[:] = [x for x in dirs if x not in _SKIP_DIRS and not x.startswith(".")]
        if "package-lock.json" in files:
            npm_dirs.append(cur)
        if any(m in files for m in _PY_MARKERS):
            py_dirs.append(cur)
    return npm_dirs, py_dirs


def detect_and_verify(d: str) -> dict:
    npm_dirs, py_dirs = _find_ecosystem_dirs(d)
    # A Dockerfile-only / base-image change has no lockfile to verify statically
    # (it needs an image build), so it is NOT auto-passed — it holds for review.
    if not npm_dirs and not py_dirs:
        return {
            "ok": False,
            "ecosystem": "unknown-or-image-only",
            "reason": "no npm/python lockfile to verify statically (e.g. Dockerfile-only base-image bump); holding for review",
            "results": [],
        }

    results = []
    for nd in npm_dirs:
        r = verify_npm(nd)
        r["dir"] = os.path.relpath(nd, d)
        results.append(r)
    for pd in py_dirs:
        r = verify_python(pd)
        r["dir"] = os.path.relpath(pd, d)
        results.append(r)
    ok = all(r["ok"] for r in results)
    return {"ok": ok, "results": results}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dir", default=".", help="repo root of the checked-out PR branch")
    args = ap.parse_args()
    verdict = detect_and_verify(os.path.abspath(args.dir))
    print(json.dumps(verdict, indent=2))
    return 0 if verdict.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
