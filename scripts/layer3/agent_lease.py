#!/usr/bin/env python3
"""Lease on a Layer 3 test agent, so two CI runs never deploy to the same agent at once.

Contract C-LEASE of YET-3659 (Layer 3). One DynamoDB item per agent, keyed by
``agent_id``; a run owns the agent while it holds the item's ``lease_token`` and
``expires_at`` is in the future. Writes are conditional, so a lease is only taken when
free or expired, and only renewed or released by its owner.

    acquire  take the lease, waiting up to --wait seconds while another run holds it
    hold     renew every --interval seconds until killed (a CircleCI background step);
             on loss writes <lease>.lost next to the lease file and exits 4
    renew    renew once
    release  give the lease back (a lease already lost or taken over is a warning)
    check    exit 4 when <lease>.lost exists

Exit codes: 0 ok, 2 usage or AWS error, 3 agent still busy after --wait, 4 lease lost.
Region and credentials come from the standard AWS environment. Needs Python >= 3.11
and boto3.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

EXIT_OK = 0
EXIT_ERROR = 2
EXIT_BUSY = 3
EXIT_LOST = 4

# The DynamoDB TTL attribute only cleans up abandoned items; correctness relies on
# expires_at alone.
CLEANUP_AFTER_SECONDS = 86400
CONDITION_FAILED = "ConditionalCheckFailedException"


class LeaseLost(Exception):
    """The lease token no longer matches the item: it expired and someone else took it."""


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def now() -> int:
    return int(time.time())


def sleep(seconds: float) -> None:
    time.sleep(seconds)


def iso_utc(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def lost_marker(lease_path: Path) -> Path:
    return lease_path.with_suffix(".lost")


def is_condition_failure(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") == CONDITION_FAILED


def read_lease(lease_path: Path) -> dict[str, Any]:
    return json.loads(lease_path.read_text())


def write_lease(lease_path: Path, lease: dict[str, Any]) -> None:
    lease_path.write_text(json.dumps(lease) + "\n")


def try_put(client: Any, table: str, item: dict[str, Any], at: int) -> bool:
    """Write the lease item if the agent is free or its lease expired; False if busy."""
    try:
        client.put_item(
            TableName=table,
            Item={
                "agent_id": {"S": item["agent_id"]},
                "lease_token": {"S": item["lease_token"]},
                "holder": {"S": item["holder"]},
                "branch": {"S": item["branch"]},
                "acquired_at": {"N": str(at)},
                "expires_at": {"N": str(item["expires_at"])},
                "ttl": {"N": str(item["expires_at"] + CLEANUP_AFTER_SECONDS)},
            },
            ConditionExpression="attribute_not_exists(agent_id) OR expires_at < :now",
            ExpressionAttributeValues={":now": {"N": str(at)}},
        )
    except ClientError as error:
        if is_condition_failure(error):
            return False
        raise
    return True


def current_holder(client: Any, table: str, agent_id: str) -> dict[str, Any] | None:
    response = client.get_item(
        TableName=table,
        Key={"agent_id": {"S": agent_id}},
        ConsistentRead=True,
        ProjectionExpression="holder, branch, expires_at",
    )
    item = response.get("Item")
    if not item:
        return None
    return {
        "holder": item.get("holder", {}).get("S", "?"),
        "branch": item.get("branch", {}).get("S", "?"),
        "expires_at": int(item.get("expires_at", {}).get("N", "0")),
    }


def busy_message(agent_id: str, holder: dict[str, Any]) -> str:
    return (
        f"agent {agent_id} busy, held by {holder['holder']} (branch {holder['branch']}) "
        f"until {iso_utc(holder['expires_at'])}"
    )


def acquire(
    client: Any,
    table: str,
    agent_id: str,
    holder: str,
    branch: str,
    ttl: int,
    wait: int,
    poll: int,
    out: Path,
) -> int:
    deadline = now() + wait
    while True:
        at = now()
        lease = {
            "agent_id": agent_id,
            "lease_token": str(uuid.uuid4()),
            "expires_at": at + ttl,
            "holder": holder,
        }
        if try_put(client, table, {**lease, "branch": branch}, at):
            write_lease(out, lease)
            # A marker from an earlier run in the same place must not fail this one.
            lost_marker(out).unlink(missing_ok=True)
            print(
                f"acquired lease on agent {agent_id} until {iso_utc(lease['expires_at'])}"
            )
            return EXIT_OK
        current = current_holder(client, table, agent_id)
        if current is None and now() <= deadline:
            continue  # released between our put and get: try again at once
        current = current or {"holder": "?", "branch": "?", "expires_at": now()}
        if now() + poll > deadline:
            log(busy_message(agent_id, current))
            return EXIT_BUSY
        log(f"waiting: {busy_message(agent_id, current)}")
        sleep(poll)


def renew_once(client: Any, table: str, lease_path: Path, ttl: int) -> dict[str, Any]:
    """Extend the lease; raises LeaseLost when the token no longer matches."""
    lease = read_lease(lease_path)
    expires_at = now() + ttl
    try:
        client.update_item(
            TableName=table,
            Key={"agent_id": {"S": lease["agent_id"]}},
            UpdateExpression="SET expires_at = :new, #ttl = :ttl",
            ConditionExpression="lease_token = :tok",
            ExpressionAttributeNames={"#ttl": "ttl"},
            ExpressionAttributeValues={
                ":new": {"N": str(expires_at)},
                ":ttl": {"N": str(expires_at + CLEANUP_AFTER_SECONDS)},
                ":tok": {"S": lease["lease_token"]},
            },
        )
    except ClientError as error:
        if is_condition_failure(error):
            raise LeaseLost() from None
        raise
    lease["expires_at"] = expires_at
    write_lease(lease_path, lease)
    return lease


def mark_lost(lease_path: Path, reason: str) -> int:
    lost_marker(lease_path).write_text(reason + "\n")
    log(f"lease lost: {reason}")
    return EXIT_LOST


def renew(client: Any, table: str, lease_path: Path, ttl: int) -> int:
    try:
        lease = renew_once(client, table, lease_path, ttl)
    except LeaseLost:
        return mark_lost(
            lease_path, "lease token no longer matches (expired and taken over)"
        )
    print(
        f"renewed lease on agent {lease['agent_id']} until {iso_utc(lease['expires_at'])}"
    )
    return EXIT_OK


def hold(
    client: Any,
    table: str,
    lease_path: Path,
    interval: int,
    ttl: int,
    max_rounds: int | None = None,
) -> int:
    """Renew every interval until killed. A failed renewal (AWS error) is retried at the
    next interval; the lease only counts as lost when the token no longer matches or
    its expiry passes without a successful renewal."""
    rounds = 0
    while max_rounds is None or rounds < max_rounds:
        rounds += 1
        sleep(interval)
        try:
            renew_once(client, table, lease_path, ttl)
        except LeaseLost:
            return mark_lost(
                lease_path, "lease token no longer matches (expired and taken over)"
            )
        except (ClientError, BotoCoreError) as error:
            expires_at = int(read_lease(lease_path)["expires_at"])
            if now() >= expires_at:
                return mark_lost(
                    lease_path, f"could not renew before {iso_utc(expires_at)}"
                )
            log(f"renew failed, retrying in {interval}s: {type(error).__name__}")
    return EXIT_OK


def release(client: Any, table: str, lease_path: Path) -> int:
    if not lease_path.exists():
        # release runs with `when: always`, also after an acquire that never got the lease.
        print(f"no lease file at {lease_path}, nothing to release")
        return EXIT_OK
    lease = read_lease(lease_path)
    try:
        client.delete_item(
            TableName=table,
            Key={"agent_id": {"S": lease["agent_id"]}},
            ConditionExpression="lease_token = :tok",
            ExpressionAttributeValues={":tok": {"S": lease["lease_token"]}},
        )
    except ClientError as error:
        if is_condition_failure(error):
            log(
                f"warning: lease on agent {lease['agent_id']} was already lost; not released"
            )
            return EXIT_OK
        raise
    print(f"released lease on agent {lease['agent_id']}")
    return EXIT_OK


def check(lease_path: Path) -> int:
    marker = lost_marker(lease_path)
    if marker.exists():
        log(f"lease lost ({marker}): {marker.read_text().strip()}")
        return EXIT_LOST
    return EXIT_OK


def positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError(f"must be > 0: {value}")
    return number


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="agent_lease.py",
        description="DynamoDB lease on a Layer 3 test agent (contract C-LEASE).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser(
        "acquire", help="take the lease, waiting while another run holds it"
    )
    p.add_argument("--table", required=True)
    p.add_argument("--agent-id", required=True)
    p.add_argument("--holder", required=True, help="CI job URL of this run")
    p.add_argument("--branch", default="")
    p.add_argument("--ttl", type=positive_int, default=600)
    p.add_argument(
        "--wait", type=int, default=2700, help="seconds to wait for a busy agent"
    )
    p.add_argument("--poll", type=positive_int, default=30)
    p.add_argument("--out", required=True, type=Path, help="lease file to write")

    p = sub.add_parser("hold", help="renew every --interval seconds until killed")
    p.add_argument("--table", required=True)
    p.add_argument("--lease", required=True, type=Path)
    p.add_argument("--interval", type=positive_int, default=60)
    p.add_argument("--ttl", type=positive_int, default=600)

    p = sub.add_parser("renew", help="renew the lease once")
    p.add_argument("--table", required=True)
    p.add_argument("--lease", required=True, type=Path)
    p.add_argument("--ttl", type=positive_int, default=600)

    p = sub.add_parser("release", help="give the lease back")
    p.add_argument("--table", required=True)
    p.add_argument("--lease", required=True, type=Path)

    p = sub.add_parser("check", help="exit 4 if the lease was lost")
    p.add_argument("--lease", required=True, type=Path)
    return parser.parse_args(argv)


def run(args: argparse.Namespace, client: Any) -> int:
    if args.command == "acquire":
        return acquire(
            client,
            args.table,
            args.agent_id,
            args.holder,
            args.branch,
            args.ttl,
            args.wait,
            args.poll,
            args.out,
        )
    if args.command == "hold":
        return hold(client, args.table, args.lease, args.interval, args.ttl)
    if args.command == "renew":
        return renew(client, args.table, args.lease, args.ttl)
    return release(client, args.table, args.lease)


def main(argv: list[str] | None = None, client: Any = None) -> int:
    args = parse_args(argv)
    if args.command == "check":
        return check(args.lease)
    if args.command in ("hold", "renew") and not args.lease.exists():
        log(f"no lease file at {args.lease}")
        return EXIT_ERROR
    try:
        return run(args, client or boto3.client("dynamodb"))
    except (ClientError, BotoCoreError) as error:
        code = (
            error.response.get("Error", {}).get("Code")
            if isinstance(error, ClientError)
            else ""
        )
        log(f"AWS error: {type(error).__name__} {code}".rstrip())
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
