#!/usr/bin/env python3
"""Run a Layer 3 agent stack in Terraform Cloud (contract C-TFC of YET-3659).

    apply    queue an API-driven run on the workspace with run variables (auto-apply),
             wait for it to finish, then write the workspace outputs
    outputs  write the workspace's current outputs

The token comes from the TFC_TOKEN environment variable only. CI logs of this public
repo are public, so the script prints the run URL, statuses and output names, never
variable or output values; outputs marked sensitive are left out of the file too.

Exit codes: 0 ok, 2 usage or HTTP error (after 3 retries), 5 run failed or canceled,
6 timeout (the run is canceled). Stdlib only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

API = "https://app.terraform.io/api/v2"
APP = "https://app.terraform.io/app"
EXIT_OK = 0
EXIT_ERROR = 2
EXIT_FAILED = 5
EXIT_TIMEOUT = 6

SUCCESS = {"applied", "planned_and_finished"}
FAILURE = {"errored", "discarded", "canceled", "force_canceled", "policy_soft_failed"}
RETRIES = 3
BACKOFF_SECONDS = (5, 15, 30)
MAX_RETRY_AFTER = 60
REQUEST_TIMEOUT = 30


class TfcError(Exception):
    """An HTTP error that retrying did not fix, or an unexpected response."""


class HttpError(Exception):
    def __init__(self, status: int, retry_after: float | None = None):
        super().__init__(f"HTTP {status}")
        self.status = status
        self.retry_after = retry_after


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def now() -> float:
    return time.monotonic()


def sleep(seconds: float) -> None:
    time.sleep(seconds)


def http(
    method: str, url: str, headers: dict[str, str], body: bytes | None = None
) -> Any:
    """One request; returns the decoded JSON body (None when empty). Raises HttpError for
    status >= 400 and urllib.error.URLError/OSError for network failures."""
    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
            raw = response.read()
    except urllib.error.HTTPError as error:
        raise HttpError(
            error.code, retry_after(error.headers.get("Retry-After"))
        ) from None
    return json.loads(raw) if raw else None


def retry_after(value: str | None) -> float | None:
    try:
        return min(max(float(value), 0.0), MAX_RETRY_AFTER) if value else None
    except ValueError:
        return None


def is_transient(error: Exception) -> bool:
    if isinstance(error, HttpError):
        return error.status == 429 or error.status >= 500
    return isinstance(error, (urllib.error.URLError, OSError, TimeoutError))


def describe(error: Exception) -> str:
    return (
        f"HTTP {error.status}" if isinstance(error, HttpError) else type(error).__name__
    )


@dataclass
class Tfc:
    token: str

    def headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json",
        }

    def call(self, method: str, path: str, payload: Any = None) -> Any:
        """Retry 429/5xx/network errors RETRIES times, then raise TfcError."""
        body = json.dumps(payload).encode() if payload is not None else None
        url = path if path.startswith(f"{API}/") else f"{API}{path}"
        for attempt in range(RETRIES + 1):
            try:
                return http(method, url, self.headers(), body)
            except ValueError:
                raise TfcError(f"{method} {path}: response was not JSON") from None
            except Exception as error:  # noqa: BLE001 - classified right below
                if not is_transient(error) or attempt == RETRIES:
                    tries = f" after {RETRIES} retries" if is_transient(error) else ""
                    raise TfcError(
                        f"{method} {path}: {describe(error)}{tries}"
                    ) from None
                wait = getattr(error, "retry_after", None)
                sleep(wait if wait is not None else BACKOFF_SECONDS[attempt])
        raise AssertionError("unreachable")


def workspace_id(tfc: Tfc, org: str, workspace: str) -> str:
    path = f"/organizations/{quote(org)}/workspaces/{quote(workspace)}"
    data = tfc.call("GET", path)
    try:
        return str(data["data"]["id"])
    except (KeyError, TypeError):
        raise TfcError(f"GET {path}: no workspace id in the response") from None


def quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


def run_url(org: str, workspace: str, run_id: str) -> str:
    return f"{APP}/{quote(org)}/workspaces/{quote(workspace)}/runs/{run_id}"


def hcl_string(value: str) -> str:
    """Run variable values are HCL expressions; a JSON string literal is a valid HCL one."""
    return json.dumps(value)


def create_run(tfc: Tfc, ws_id: str, variables: dict[str, str], message: str) -> str:
    payload = {
        "data": {
            "type": "runs",
            "attributes": {
                "message": message,
                "auto-apply": True,
                "variables": [
                    {"key": k, "value": hcl_string(v)} for k, v in variables.items()
                ],
            },
            "relationships": {
                "workspace": {"data": {"type": "workspaces", "id": ws_id}}
            },
        }
    }
    data = tfc.call("POST", "/runs", payload)
    try:
        return str(data["data"]["id"])
    except (KeyError, TypeError):
        raise TfcError("POST /runs: no run id in the response") from None


def run_status(tfc: Tfc, run_id: str) -> str:
    data = tfc.call("GET", f"/runs/{run_id}")
    try:
        return str(data["data"]["attributes"]["status"])
    except (KeyError, TypeError):
        raise TfcError(f"GET /runs/{run_id}: no status in the response") from None


def cancel_run(tfc: Tfc, run_id: str) -> None:
    """Stop a timed-out run so it can't apply later. Cancel only works while planning or
    applying; a run still queued or waiting needs discard instead."""
    comment = {"comment": "tfc_run.py timeout"}
    for action in ("cancel", "discard"):
        try:
            tfc.call("POST", f"/runs/{run_id}/actions/{action}", comment)
            log(f"run {action} requested")
            return
        except TfcError as error:
            log(f"run {action} failed: {error}")


def current_outputs(tfc: Tfc, ws_id: str) -> dict[str, Any]:
    """Non-sensitive outputs of the workspace's current state version; sensitive ones are
    reported by name only and left out."""
    outputs: dict[str, Any] = {}
    path: str | None = f"/workspaces/{ws_id}/current-state-version-outputs"
    while path:
        data = tfc.call("GET", path)
        if not isinstance(data, dict) or not isinstance(data.get("data"), list):
            raise TfcError("current-state-version-outputs: unexpected response")
        for item in data["data"]:
            attributes = item.get("attributes") or {}
            name = attributes.get("name")
            if not name:
                continue
            if attributes.get("sensitive"):
                log(f"output {name} is sensitive; left out")
                continue
            outputs[name] = attributes.get("value")
        path = (data.get("links") or {}).get("next")
        if path and not path.startswith(f"{API}/"):
            raise TfcError(
                "current-state-version-outputs: next page is not on the TFC API"
            )
    print(f"outputs: {', '.join(sorted(outputs)) or '(none)'}")
    return outputs


def write_file(path: Path, content: dict[str, Any]) -> None:
    path.write_text(json.dumps(content, indent=2) + "\n")


def apply(
    tfc: Tfc,
    org: str,
    workspace: str,
    variables: dict[str, str],
    message: str,
    timeout: int,
    poll: int,
    outputs_file: Path,
) -> int:
    ws_id = workspace_id(tfc, org, workspace)
    run_id = create_run(tfc, ws_id, variables, message)
    url = run_url(org, workspace, run_id)
    names = ", ".join(sorted(variables)) or "(none)"
    print(f"run {run_id} queued on {workspace} (variables: {names}): {url}", flush=True)
    deadline = now() + timeout
    last = ""
    while True:
        status = run_status(tfc, run_id)
        if status != last:
            print(f"status: {status}", flush=True)
            last = status
        if status in SUCCESS:
            break
        if status in FAILURE:
            log(f"run {status}: {url}")
            return EXIT_FAILED
        if now() + poll > deadline:
            log(f"timed out after {timeout}s in status {status}: {url}")
            cancel_run(tfc, run_id)
            return EXIT_TIMEOUT
        sleep(poll)
    outputs = current_outputs(tfc, ws_id)
    write_file(
        outputs_file,
        {"run_id": run_id, "run_url": url, "status": status, "outputs": outputs},
    )
    return EXIT_OK


def parse_var(value: str) -> tuple[str, str]:
    key, sep, val = value.partition("=")
    if not sep or not key:
        raise argparse.ArgumentTypeError(
            f"expected KEY=VALUE: {value.split('=', 1)[0]}"
        )
    return key, val


def positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError(f"must be > 0: {value}")
    return number


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tfc_run.py",
        description="Terraform Cloud runs for the Layer 3 agent stacks (contract C-TFC).",
        epilog="Env: TFC_TOKEN (required).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("apply", help="queue an auto-apply run, wait, write the outputs")
    p.add_argument("--org", required=True)
    p.add_argument("--workspace", required=True)
    p.add_argument(
        "--var",
        dest="variables",
        type=parse_var,
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="run variable (string), repeatable",
    )
    p.add_argument(
        "--message", default="Triggered by tfc_run.py", help="run message (job URL)"
    )
    p.add_argument("--timeout", type=positive_int, default=1800)
    p.add_argument("--poll", type=positive_int, default=10)
    p.add_argument("--outputs-file", required=True, type=Path)

    p = sub.add_parser("outputs", help="write the workspace's current outputs")
    p.add_argument("--org", required=True)
    p.add_argument("--workspace", required=True)
    p.add_argument("--outputs-file", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    token = os.environ.get("TFC_TOKEN", "")
    if not token:
        log("error: TFC_TOKEN must be set")
        return EXIT_ERROR
    tfc = Tfc(token)
    try:
        if args.command == "apply":
            return apply(
                tfc,
                args.org,
                args.workspace,
                dict(args.variables),
                args.message,
                args.timeout,
                args.poll,
                args.outputs_file,
            )
        ws_id = workspace_id(tfc, args.org, args.workspace)
        write_file(args.outputs_file, {"outputs": current_outputs(tfc, ws_id)})
        return EXIT_OK
    except TfcError as error:
        log(f"error: {error}")
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
