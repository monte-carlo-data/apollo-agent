#!/usr/bin/env python3
"""Check a Layer 3 test agent through the Monte Carlo API (contract C-MC of YET-3659).

    health    poll the agent's reachability check until it reports the expected version,
              build and (optionally) image tag
    validate  run the connection validations of the given connection types and write a
              report

Environment: MCD_API_ID, MCD_API_TOKEN, MCD_API_BASE (default the dev API).

What reaches the log is deliberately narrow, because CI logs of this public repo are
public: health prints only version, build and image tag (the reachability response also
carries the agent's environment); validate prints connection types, outcomes and the
validations' friendly messages, never raw responses, stack traces or additional_data.

Exit codes: health 0 match, 6 timeout; validate 0 no FAIL, 1 any FAIL; 2 usage or
credentials. Stdlib only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_BASE = "https://api.dev.getmontecarlo.com"
EXIT_OK = 0
EXIT_FAIL = 1
EXIT_USAGE = 2
EXIT_TIMEOUT = 6

HEALTH_QUERY = (
    "query($id: UUID!){ testAgentReachability(agentId:$id, "
    'validationName:"validate_agent_reachability"){ success additionalData{ returnedData } } }'
)
# The reachability check goes through the agent; the CLI allows it 40s.
HEALTH_REQUEST_TIMEOUT = 60
REQUEST_TIMEOUT = 30
RETRIES = 3
BACKOFF_SECONDS = (5, 15, 30)
MAX_RETRY_AFTER = 60
DEFAULT_RUN_POLL = 3
MESSAGE_LIMIT = 300

PASS, FAIL, WARN, SKIP, BACKEND, NOT_COVERED = (
    "PASS",
    "FAIL",
    "WARN",
    "SKIP",
    "BACKEND",
    "NOT_COVERED",
)


class UsageError(Exception):
    """Bad arguments, input files or credentials."""


class HttpError(Exception):
    def __init__(self, status: int, retry_after: float | None = None):
        super().__init__(f"HTTP {status}")
        self.status = status
        self.retry_after = retry_after


class BackendError(Exception):
    """The API did not answer usably after the retries (rate limit, 5xx, network, deadline)."""


@dataclass
class Response:
    status: int
    headers: dict[str, str]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body or b"null")


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def now() -> float:
    return time.monotonic()


def sleep(seconds: float) -> None:
    time.sleep(seconds)


def http(
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes | None = None,
    timeout: float = REQUEST_TIMEOUT,
) -> Response:
    """One HTTP request. Raises HttpError for status >= 400, urllib.error.URLError or
    OSError for network failures."""
    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return Response(
                response.status, dict(response.headers.items()), response.read()
            )
    except urllib.error.HTTPError as error:
        raise HttpError(error.code, retry_after(dict(error.headers.items()))) from None


def retry_after(headers: dict[str, str]) -> float | None:
    for key, value in headers.items():
        if key.lower() == "retry-after":
            try:
                return min(max(float(value), 0.0), MAX_RETRY_AFTER)
            except ValueError:
                return None
    return None


def is_transient(error: Exception) -> bool:
    if isinstance(error, HttpError):
        return error.status == 429 or error.status >= 500
    return isinstance(error, (urllib.error.URLError, OSError, TimeoutError))


def describe(error: Exception) -> str:
    if isinstance(error, HttpError):
        return f"HTTP {error.status}"
    return type(error).__name__


def request_with_retry(
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes | None = None,
    timeout: float = REQUEST_TIMEOUT,
) -> Response:
    """Retry 429/5xx/network errors RETRIES times (honoring Retry-After), then raise
    BackendError. Other HTTP errors raise HttpError at once."""
    for attempt in range(RETRIES + 1):
        try:
            return http(method, url, headers, body, timeout)
        except Exception as error:  # noqa: BLE001 - classified right below
            if not is_transient(error):
                raise
            if attempt == RETRIES:
                raise BackendError(
                    f"{describe(error)} after {RETRIES} retries"
                ) from None
            wait = getattr(error, "retry_after", None)
            sleep(wait if wait is not None else BACKOFF_SECONDS[attempt])
    raise AssertionError("unreachable")


@dataclass
class Api:
    base: str
    api_id: str
    token: str

    @classmethod
    def from_env(cls) -> Api:
        api_id = os.environ.get("MCD_API_ID", "")
        token = os.environ.get("MCD_API_TOKEN", "")
        if not api_id or not token:
            raise UsageError("MCD_API_ID and MCD_API_TOKEN must be set")
        base = os.environ.get("MCD_API_BASE", "") or DEFAULT_BASE
        return cls(base.rstrip("/"), api_id, token)

    def graphql_headers(self) -> dict[str, str]:
        return {
            "x-mcd-id": self.api_id,
            "x-mcd-token": self.token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def rest_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_id}:{self.token}",
            "Accept": "application/json",
        }


# ---------------------------------------------------------------------------------------
# health


@dataclass(frozen=True)
class Health:
    version: str
    build: str
    image_tag: str

    def line(self) -> str:
        return f"version={self.version} build={self.build} image_tag={self.image_tag}"


def image_tag(image: str) -> str:
    """Tag of an image reference; Azure reports it as ``DOCKER|<ref>``."""
    ref = image.split("|", 1)[1] if image.upper().startswith("DOCKER|") else image
    ref = ref.split("@", 1)[0]
    name = ref.rsplit("/", 1)[-1]
    return name.rsplit(":", 1)[1] if ":" in name else ""


def parse_health(payload: Any) -> Health | None:
    """Keep only version, build and the image tag of a reachability response; None when
    the agent was not reached. The rest of the payload is dropped unlogged."""
    try:
        result = payload["data"]["testAgentReachability"]
    except (KeyError, TypeError):
        return None
    if not isinstance(result, dict) or not result.get("success"):
        return None
    returned = (result.get("additionalData") or {}).get("returnedData")
    if isinstance(returned, str):
        try:
            returned = json.loads(returned)
        except ValueError:
            return None
    if not isinstance(returned, dict):
        return None
    platform_info = returned.get("platform_info")
    image = platform_info.get("image") if isinstance(platform_info, dict) else None
    return Health(
        version=str(returned.get("version") or ""),
        build=str(returned.get("build") or ""),
        image_tag=image_tag(image) if isinstance(image, str) else "",
    )


def health_poll(api: Api, agent_id: str) -> tuple[Health | None, str]:
    """One reachability check: (health, reason when there is none)."""
    body = json.dumps({"query": HEALTH_QUERY, "variables": {"id": agent_id}}).encode()
    try:
        response = http(
            "POST",
            f"{api.base}/graphql",
            api.graphql_headers(),
            body,
            HEALTH_REQUEST_TIMEOUT,
        )
        payload = response.json()
    except HttpError as error:
        if error.status in (401, 403):
            raise UsageError(
                f"Monte Carlo API rejected the credentials ({describe(error)})"
            )
        return None, describe(error)
    except (urllib.error.URLError, OSError, ValueError) as error:
        return None, describe(error)
    if isinstance(payload, dict) and payload.get("errors"):
        return None, f"GraphQL errors: {len(payload['errors'])}"
    found = parse_health(payload)
    return found, "" if found else "agent not reachable"


def health(
    api: Api,
    agent_id: str,
    version: str,
    build: str,
    tag: str | None,
    initial_wait: int,
    timeout: int,
    poll: int,
) -> int:
    expected = Health(version, build, tag or "")
    if initial_wait > 0:
        print(f"waiting {initial_wait}s before the first health check")
        sleep(initial_wait)
    deadline = now() + timeout
    while True:
        found, reason = health_poll(api, agent_id)
        print(found.line() if found else f"health check: {reason}", flush=True)
        if found and found.version == version and found.build == build:
            if tag is None or found.image_tag == tag:
                print("agent reports the expected version")
                return EXIT_OK
        if now() + poll > deadline:
            log(f"timed out after {timeout}s waiting for {expected.line()}")
            return EXIT_TIMEOUT
        sleep(poll)


# ---------------------------------------------------------------------------------------
# validate


@dataclass
class Result:
    connection_type: str
    connection_id: str | None
    outcome: str
    failed_validations: list[dict[str, str]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "connection_type": self.connection_type,
            "connection_id": self.connection_id,
            "outcome": self.outcome,
            "failed_validations": self.failed_validations,
        }


def first_message(items: Any) -> str:
    for item in items or []:
        if isinstance(item, dict):
            message = item.get("friendly_message") or item.get("cause")
            if message:
                return str(message)[:MESSAGE_LIMIT]
    return ""


def classify_validation(validation: dict[str, Any]) -> str:
    """Per validation, as the data-collector smoke test classifies them."""
    status = validation.get("status")
    if status == "skipped":
        return SKIP
    if (
        validation.get("passed") is False
        or validation.get("errors")
        or status != "completed"
    ):
        return FAIL
    if validation.get("warnings"):
        return WARN
    return PASS


def classify_run(
    connection_type: str, connection_id: str, run: dict[str, Any]
) -> Result:
    """Connection outcome: FAIL if any validation failed, else WARN if any warned, else
    SKIP if every validation was skipped, else PASS. Lists only name and friendly message
    of the failed (or, for WARN, warned) validations."""
    validations = [v for v in run.get("validations") or [] if isinstance(v, dict)]
    by_outcome: dict[str, list[dict[str, Any]]] = {}
    for validation in validations:
        by_outcome.setdefault(classify_validation(validation), []).append(validation)
    if FAIL in by_outcome:
        outcome, listed, key = FAIL, by_outcome[FAIL], "errors"
    elif WARN in by_outcome:
        outcome, listed, key = WARN, by_outcome[WARN], "warnings"
    elif validations and set(by_outcome) == {SKIP}:
        outcome, listed, key = SKIP, [], ""
    elif not validations:
        outcome, listed, key = FAIL, [], ""
    else:
        outcome, listed, key = PASS, [], ""
    failed = [
        {
            "name": str(v.get("name") or "?"),
            "friendly_message": first_message(v.get(key)) or str(v.get("status") or ""),
        }
        for v in listed
    ]
    if outcome == FAIL and not validations:
        failed = [
            {"name": "(run)", "friendly_message": "the run reported no validations"}
        ]
    return Result(connection_type, connection_id, outcome, failed)


def run_validation(api: Api, connection_id: str, deadline: float) -> dict[str, Any]:
    """Start a validation run and poll it until completed; raises BackendError when the
    API keeps failing or the run is still going at the deadline."""
    response = request_with_retry(
        "POST",
        f"{api.base}/api/v2/connections/{connection_id}/validate",
        api.rest_headers(),
    )
    run = response.json()
    run_id = run.get("id") if isinstance(run, dict) else None
    if not run_id:
        raise BackendError("validate returned no run id")
    while run.get("status") != "completed":
        wait = retry_after(response.headers)
        wait = DEFAULT_RUN_POLL if wait is None else wait
        if now() + wait > deadline:
            raise BackendError("run still running at the deadline")
        sleep(wait)
        response = request_with_retry(
            "GET", f"{api.base}/api/v2/validations/{run_id}", api.rest_headers()
        )
        run = response.json()
        if not isinstance(run, dict):
            raise BackendError("unexpected validation run response")
    return run


def validate_one(
    api: Api, connection_type: str, connection_id: str, timeout: int
) -> Result:
    deadline = now() + timeout
    try:
        run = run_validation(api, connection_id, deadline)
    except BackendError as error:
        message = str(error)
        return Result(
            connection_type,
            connection_id,
            BACKEND,
            [{"name": "(run)", "friendly_message": message}],
        )
    except HttpError as error:
        return Result(
            connection_type,
            connection_id,
            FAIL,
            [
                {
                    "name": "(validate)",
                    "friendly_message": f"API answered {describe(error)}",
                }
            ],
        )
    except ValueError:
        return Result(
            connection_type,
            connection_id,
            BACKEND,
            [{"name": "(run)", "friendly_message": "response was not JSON"}],
        )
    return classify_run(connection_type, connection_id, run)


def load_connection_ids(spec: str) -> tuple[dict[str, str], str | None]:
    """``FILE[:dotted.path]`` → (type → connection id, platform). The platform is read
    from a ``platform`` key next to the map (tfc_run.py outputs carry both)."""
    path, _, dotted = spec.partition(":")
    try:
        data: Any = json.loads(Path(path).read_text())
    except (OSError, ValueError) as error:
        raise UsageError(f"cannot read {path}: {type(error).__name__}") from None
    parent: Any = None
    for key in [k for k in dotted.split(".") if k]:
        if not isinstance(data, dict) or key not in data:
            raise UsageError(f"{path} has no {dotted}")
        parent, data = data, data[key]
    if not isinstance(data, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in data.items()
    ):
        raise UsageError(f"{spec} is not a map of connection type to id")
    platform = parent.get("platform") if isinstance(parent, dict) else None
    return data, platform if isinstance(platform, str) else None


def parse_types(value: str) -> list[str]:
    types: list[str] = []
    for item in value.split(","):
        item = item.strip()
        if item and item not in types:
            types.append(item)
    return types


def validate(
    api: Api, spec: str, types: list[str], concurrency: int, timeout: int, report: Path
) -> int:
    connection_ids, platform = load_connection_ids(spec)
    covered = [t for t in types if t in connection_ids]
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {
            t: pool.submit(validate_one, api, t, connection_ids[t], timeout)
            for t in covered
        }
        results = [
            (futures[t].result() if t in futures else Result(t, None, NOT_COVERED, []))
            for t in types
        ]
    for result in results:
        print(f"{result.connection_type}: {result.outcome}")
        for failed in result.failed_validations:
            print(f"  - {failed['name']}: {failed['friendly_message']}")
    report.write_text(
        json.dumps(
            {"platform": platform, "results": [r.as_dict() for r in results]}, indent=2
        )
        + "\n"
    )
    failures = sum(r.outcome == FAIL for r in results)
    counts = {
        o: sum(r.outcome == o for r in results) for o in (PASS, WARN, SKIP, BACKEND)
    }
    counts[NOT_COVERED] = len(types) - len(covered)
    summary = " ".join(f"{k}={v}" for k, v in {FAIL: failures, **counts}.items())
    print(f"summary: {summary}")
    return EXIT_FAIL if failures else EXIT_OK


# ---------------------------------------------------------------------------------------
# CLI


def positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError(f"must be > 0: {value}")
    return number


def non_negative_int(value: str) -> int:
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError(f"must be >= 0: {value}")
    return number


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="mc_check.py",
        description="Health and connection validation of a Layer 3 test agent (contract C-MC).",
        epilog="Env: MCD_API_ID, MCD_API_TOKEN, MCD_API_BASE (default %s)."
        % DEFAULT_BASE,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser(
        "health", help="wait until the agent reports the expected version"
    )
    p.add_argument("--agent-id", required=True)
    p.add_argument("--expect-version", required=True)
    p.add_argument("--expect-build", required=True)
    p.add_argument("--expect-image-tag", default=None)
    p.add_argument("--initial-wait", type=non_negative_int, default=0)
    p.add_argument("--timeout", type=positive_int, default=600)
    p.add_argument("--poll", type=positive_int, default=15)

    p = sub.add_parser("validate", help="validate connections and write a report")
    p.add_argument(
        "--connection-ids",
        required=True,
        metavar="FILE[:PATH]",
        help="JSON file and dotted path of the type -> connection id map",
    )
    p.add_argument(
        "--types", required=True, type=parse_types, help="comma-separated types"
    )
    p.add_argument("--concurrency", type=positive_int, default=3)
    p.add_argument(
        "--timeout", type=positive_int, default=900, help="seconds per connection"
    )
    p.add_argument("--report", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        api = Api.from_env()
        if args.command == "health":
            return health(
                api,
                args.agent_id,
                args.expect_version,
                args.expect_build,
                args.expect_image_tag,
                args.initial_wait,
                args.timeout,
                args.poll,
            )
        return validate(
            api,
            args.connection_ids,
            args.types,
            args.concurrency,
            args.timeout,
            args.report,
        )
    except UsageError as error:
        log(f"error: {error}")
        return EXIT_USAGE


if __name__ == "__main__":
    sys.exit(main())
