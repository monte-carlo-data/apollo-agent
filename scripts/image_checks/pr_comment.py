#!/usr/bin/env python3
"""Post the impact map's manual checklist as one sticky PR comment. Never fails.

Usage: python3 scripts/image_checks/pr_comment.py --body-file /tmp/impact_checklist.md

The checklist is advice for a person, not a gate, so every problem (no token, not a
PR build, GitHub API errors) is logged and the script still exits 0: a failing step
would make the orchestrator bot spend CI-fix attempts on it.

Needs a token with pull-request write access in ``IMPACT_MAP_GITHUB_TOKEN``; without
it this is a no-op. The comment is found again through ``MARKER`` and edited in place.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

MARKER = "<!-- apollo-agent-impact-map-checklist -->"
TOKEN_ENV = "IMPACT_MAP_GITHUB_TOKEN"
API = "https://api.github.com"
EMPTY_BODY = "No manual checks needed for the latest commit."


def log(message: str) -> None:
    print(f"pr-comment: {message}", flush=True)


def pr_number(pull_request_url: str | None) -> int | None:
    match = re.search(r"/pull/(\d+)$", pull_request_url or "")
    return int(match.group(1)) if match else None


def comment_body(checklist: str) -> str:
    text = checklist.strip() or EMPTY_BODY
    return f"{MARKER}\n{text}\n"


class GitHub:
    def __init__(self, token: str, repo: str) -> None:
        self.token = token
        self.repo = repo

    def request(
        self, method: str, path: str, payload: dict[str, Any] | None = None
    ) -> Any:
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(
            f"{API}{path}",
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "User-Agent": "apollo-agent-ci",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode() or "null")

    def find_comment(self, number: int) -> int | None:
        page = 1
        while True:
            comments = self.request(
                "GET",
                f"/repos/{self.repo}/issues/{number}/comments?per_page=100&page={page}",
            )
            for comment in comments:
                if MARKER in (comment.get("body") or ""):
                    return comment["id"]
            if len(comments) < 100:
                return None
            page += 1

    def upsert(self, number: int, checklist: str) -> str:
        existing = self.find_comment(number)
        if existing is None:
            if not checklist.strip():
                return "nothing to check and no earlier comment; not posting"
            self.request(
                "POST",
                f"/repos/{self.repo}/issues/{number}/comments",
                {"body": comment_body(checklist)},
            )
            return "posted the checklist"
        self.request(
            "PATCH",
            f"/repos/{self.repo}/issues/comments/{existing}",
            {"body": comment_body(checklist)},
        )
        return "updated the checklist"


def run(body_file: Path, env: dict[str, str]) -> str:
    token = env.get(TOKEN_ENV)
    if not token:
        return f"skipped: {TOKEN_ENV} is not set"
    number = pr_number(env.get("CIRCLE_PULL_REQUEST"))
    if number is None:
        return "skipped: not a pull request build"
    repo = f"{env.get('CIRCLE_PROJECT_USERNAME')}/{env.get('CIRCLE_PROJECT_REPONAME')}"
    if not body_file.is_file():
        return "skipped: no checklist for this build (impact map not run or failed)"
    checklist = body_file.read_text()
    return GitHub(token, repo).upsert(number, checklist)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--body-file", required=True, type=Path)
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    try:
        log(run(args.body_file, dict(os.environ)))
    except urllib.error.HTTPError as exc:
        log(f"GitHub API error {exc.code} {exc.reason}; ignored")
    except Exception as exc:  # never fail the pipeline over an advisory comment
        log(f"{type(exc).__name__}: {exc}; ignored")
    return 0


if __name__ == "__main__":
    sys.exit(main())
