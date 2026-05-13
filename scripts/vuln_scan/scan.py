"""Apollo Agent nightly vulnerability scan.

Runs Docker Scout against the production `latest-*` tags of montecarlodata/agent,
diffs results against the previous run (state stored in S3), and posts a Slack
notification when there are deltas in HIGH/CRITICAL findings (or scan errors).

Designed to run from CircleCI on a daily schedule. See .circleci/vuln-scan-config.yml.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
import requests
from slack_sdk import WebClient


# --- Configuration -----------------------------------------------------------
#
# Repo, tag list, S3 bucket, and Slack channel are all CLI args (see
# parse_args) — pinned by the pipeline so they can change (dedicated channel,
# different AWS account, different image repo or tag matrix) without a code
# edit. See .circleci/vuln-scan-config.yml for the apollo-agent values.

# Days after which we re-notify on a still-open HIGH/CRITICAL CVE.
REMINDER_DAYS = 4

# Docker Scout indexes a newly-pushed image asynchronously; running `scout cves`
# before indexing completes can return an empty or partial result, which would
# look like all previous CVEs were resolved. Skip any image whose tag was pushed
# more recently than this. The next nightly run picks it up.
FRESHNESS_GRACE_HOURS = 6

# Severities we list individually in Slack; others are summarized as counts.
DETAILED_SEVERITIES = ("CRITICAL", "HIGH")
ALL_SEVERITIES = ("CRITICAL", "HIGH", "MEDIUM", "LOW", "UNKNOWN")

# Slack mrkdwn section text has a 3000-char limit; cap the per-section bullet
# count to stay well under that and keep the message scannable.
MAX_ITEMS_PER_SECTION = 40


# --- Data structures ----------------------------------------------------------


@dataclass(frozen=True)
class FindingKey:
    cve_id: str
    package: str
    installed: str


@dataclass
class Finding:
    cve_id: str
    package: str
    installed: str
    severity: str  # CRITICAL | HIGH | MEDIUM | LOW | UNKNOWN
    fixed_in: str | None

    @property
    def key(self) -> FindingKey:
        return FindingKey(self.cve_id, self.package, self.installed)


@dataclass
class DiffSummary:
    new_detailed: list[tuple[str, Finding]] = field(default_factory=list)
    reminders: list[tuple[str, Finding, str]] = field(default_factory=list)
    resolved_detailed: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    new_by_severity: dict[str, int] = field(default_factory=dict)
    totals_by_severity: dict[str, int] = field(default_factory=dict)
    # Tags we didn't scan this run (e.g. pushed too recently for Scout to have
    # indexed). Their previous state is preserved verbatim — no false deltas.
    skipped: list[tuple[str, str]] = field(default_factory=list)  # (tag, reason)


# --- Docker Hub: tag -> digest -----------------------------------------------


@dataclass
class TagInfo:
    digest: str
    last_pushed: datetime


def get_tag_info(repo: str, tag: str) -> TagInfo:
    """Look up the current digest and last-pushed timestamp for a tag."""
    url = f"https://hub.docker.com/v2/repositories/{repo}/tags/{tag}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    digest = data.get("digest")
    if not digest and data.get("images"):
        digest = data["images"][0].get("digest")
    if not digest:
        raise RuntimeError(f"No digest found for {repo}:{tag}")
    pushed_raw = (
        data.get("tag_last_pushed")
        or data.get("last_updated")
        or data.get("last_pushed")
    )
    if not pushed_raw:
        raise RuntimeError(f"No last_pushed timestamp found for {repo}:{tag}")
    return TagInfo(digest=digest, last_pushed=_parse_iso(pushed_raw))


# --- Docker Scout -------------------------------------------------------------


def scan_with_scout(repo: str, digest: str) -> list[Finding]:
    """Run docker scout cves and return parsed findings."""
    image_ref = f"{repo}@{digest}"
    proc = subprocess.run(
        ["docker", "scout", "cves", image_ref, "--format", "sarif"],
        capture_output=True,
        text=True,
        check=True,
    )
    return parse_sarif(proc.stdout)


def parse_sarif(sarif_text: str) -> list[Finding]:
    """Parse a Docker Scout SARIF document into Finding records.

    Scout puts CVE metadata in `tool.driver.rules` (severity, fixed version) and
    per-package matches in `results` (location URI is a Package URL).
    """
    sarif = json.loads(sarif_text)
    findings: list[Finding] = []

    for run in sarif.get("runs", []):
        rules_by_id: dict[str, dict[str, Any]] = {
            r["id"]: r for r in run.get("tool", {}).get("driver", {}).get("rules", [])
        }
        for result in run.get("results", []):
            rule_id = result.get("ruleId", "")
            rule = rules_by_id.get(rule_id, {})
            severity = _severity_from_rule(rule, result)
            pkg, installed = _pkg_from_location(result)
            fixed_in = _fixed_in_from_rule(rule)
            findings.append(
                Finding(
                    cve_id=rule_id,
                    package=pkg,
                    installed=installed,
                    severity=severity,
                    fixed_in=fixed_in,
                )
            )
    return findings


def _severity_from_rule(rule: dict[str, Any], result: dict[str, Any]) -> str:
    props = rule.get("properties", {})
    # CVSS score is the most reliable source.
    cvss = props.get("cvss")
    if isinstance(cvss, dict):
        score = (
            cvss.get("v3", {}).get("score")
            if isinstance(cvss.get("v3"), dict)
            else cvss.get("score")
        )
        if score is not None:
            try:
                return _cvss_to_severity(float(score))
            except (TypeError, ValueError):
                pass
    sec_sev = props.get("security-severity")
    if sec_sev is not None:
        try:
            return _cvss_to_severity(float(sec_sev))
        except (TypeError, ValueError):
            pass
    for tag in props.get("tags") or []:
        upper = str(tag).upper()
        if upper in ALL_SEVERITIES:
            return upper
    # SARIF level is a coarser fallback.
    level = result.get("level", "warning")
    return {"error": "HIGH", "warning": "MEDIUM", "note": "LOW"}.get(level, "UNKNOWN")


def _cvss_to_severity(score: float) -> str:
    if score >= 9.0:
        return "CRITICAL"
    if score >= 7.0:
        return "HIGH"
    if score >= 4.0:
        return "MEDIUM"
    if score > 0:
        return "LOW"
    return "UNKNOWN"


def _pkg_from_location(result: dict[str, Any]) -> tuple[str, str]:
    locs = result.get("locations") or []
    if not locs:
        return ("?", "?")
    uri = locs[0].get("physicalLocation", {}).get("artifactLocation", {}).get("uri", "")
    return _parse_purl(uri)


def _parse_purl(purl: str) -> tuple[str, str]:
    # pkg:<type>/[<namespace>/]<name>@<version>[?qualifiers]
    if not purl.startswith("pkg:"):
        return (purl or "?", "?")
    body = purl[len("pkg:") :]
    name_ver = body.split("?", 1)[0]
    if "@" not in name_ver:
        return (name_ver, "?")
    name, ver = name_ver.rsplit("@", 1)
    pkg = name.rsplit("/", 1)[-1]
    return (pkg, ver)


def _fixed_in_from_rule(rule: dict[str, Any]) -> str | None:
    props = rule.get("properties", {})
    for key in ("fixed-version", "fixed_version", "fix-version", "fixVersion"):
        if props.get(key):
            return str(props[key])
    return None


# --- State (S3) ---------------------------------------------------------------


def load_state(bucket: str, key: str, s3: Any = None) -> dict[str, Any]:
    s3 = s3 or boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {"version": 1, "last_run": None, "images": {}}


def save_state(state: dict[str, Any], bucket: str, key: str, s3: Any = None) -> None:
    s3 = s3 or boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(state, indent=2, sort_keys=True).encode(),
        ContentType="application/json",
    )


# --- Diffing ------------------------------------------------------------------


def diff_and_update_state(
    tag_findings: dict[str, list[Finding]],
    tag_digests: dict[str, str],
    state: dict[str, Any],
    now: datetime,
    skipped: list[tuple[str, str]] | None = None,
) -> tuple[DiffSummary, dict[str, Any]]:
    summary = DiffSummary(skipped=list(skipped or []))
    now_iso = now.isoformat()
    new_state: dict[str, Any] = {"version": 1, "last_run": now_iso, "images": {}}

    # For tags we skipped this run, preserve the previous state entry verbatim
    # so that next run's diff is correct and no false-resolved deltas appear.
    for tag, _reason in summary.skipped:
        prev_image = state.get("images", {}).get(tag)
        if prev_image is not None:
            new_state["images"][tag] = prev_image

    for tag, findings in tag_findings.items():
        prev_image = state.get("images", {}).get(tag, {})
        prev_by_key: dict[FindingKey, dict[str, Any]] = {
            FindingKey(v["cve_id"], v["package"], v["installed"]): v
            for v in prev_image.get("vulns", [])
        }

        new_records: list[dict[str, Any]] = []
        current_keys: set[FindingKey] = set()

        for f in findings:
            current_keys.add(f.key)
            summary.totals_by_severity[f.severity] = (
                summary.totals_by_severity.get(f.severity, 0) + 1
            )

            prev = prev_by_key.get(f.key)
            if prev is None:
                # First time we've seen this CVE on this tag.
                summary.new_by_severity[f.severity] = (
                    summary.new_by_severity.get(f.severity, 0) + 1
                )
                if f.severity in DETAILED_SEVERITIES:
                    summary.new_detailed.append((tag, f))
                first_seen = now_iso
                last_notified = now_iso
            else:
                first_seen = prev.get("first_seen", now_iso)
                last_notified = prev.get("last_notified", first_seen)
                if f.severity in DETAILED_SEVERITIES:
                    last_dt = _parse_iso(last_notified)
                    if (now - last_dt).days >= REMINDER_DAYS:
                        summary.reminders.append((tag, f, first_seen))
                        last_notified = now_iso

            new_records.append(
                {
                    "cve_id": f.cve_id,
                    "package": f.package,
                    "installed": f.installed,
                    "severity": f.severity,
                    "fixed_in": f.fixed_in,
                    "first_seen": first_seen,
                    "last_notified": last_notified,
                }
            )

        # Anything in the previous state that's no longer present was resolved.
        for k, v in prev_by_key.items():
            if k not in current_keys and v.get("severity") in DETAILED_SEVERITIES:
                summary.resolved_detailed.append((tag, v))

        new_state["images"][tag] = {
            "digest": tag_digests[tag],
            "vulns": new_records,
        }

    return summary, new_state


def _parse_iso(s: str) -> datetime:
    # Tolerate both "...Z" and "...+00:00" forms.
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


# --- Slack --------------------------------------------------------------------


def build_slack_blocks(
    summary: DiffSummary,
    scan_errors: list[tuple[str, str]],
    tag_digests: dict[str, str],
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Apollo Agent — Vulnerability Scan",
            },
        }
    ]

    if scan_errors:
        err_lines = "\n".join(f"• `{ref}`: {msg}" for ref, msg in scan_errors)
        blocks.append(_section(f":x: *Scan errors ({len(scan_errors)})*\n{err_lines}"))

    if summary.new_detailed:
        blocks.extend(
            _findings_section(
                f":warning: *New HIGH/CRITICAL ({len(summary.new_detailed)})*",
                [_format_new(tag, f) for tag, f in summary.new_detailed],
            )
        )

    if summary.reminders:
        blocks.extend(
            _findings_section(
                f":hourglass: *Still open ≥{REMINDER_DAYS}d ({len(summary.reminders)})*",
                [_format_reminder(tag, f, fs) for tag, f, fs in summary.reminders],
            )
        )

    if summary.resolved_detailed:
        blocks.extend(
            _findings_section(
                f":white_check_mark: *Resolved ({len(summary.resolved_detailed)})*",
                [_format_resolved(tag, v) for tag, v in summary.resolved_detailed],
            )
        )

    # Totals (all severities) and scanned images.
    totals_line = " · ".join(
        f"{sev}: {summary.totals_by_severity.get(sev, 0)} "
        f"({summary.new_by_severity.get(sev, 0)} new)"
        for sev in ALL_SEVERITIES
    )
    blocks.append({"type": "divider"})
    blocks.append(
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"Totals: {totals_line}"}],
        }
    )
    scanned_line = " · ".join(f"`{tag}@{tag_digests[tag][:19]}`" for tag in tag_digests)
    blocks.append(
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"Scanned: {scanned_line}"}],
        }
    )
    if summary.skipped:
        skipped_line = " · ".join(
            f"`{tag}` ({reason})" for tag, reason in summary.skipped
        )
        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"Skipped: {skipped_line}"}],
            }
        )
    return blocks


def _section(text: str) -> dict[str, Any]:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _findings_section(header: str, lines: list[str]) -> list[dict[str, Any]]:
    """Split a long bullet list across multiple section blocks if needed."""
    blocks: list[dict[str, Any]] = []
    first = True
    for chunk_start in range(0, len(lines), MAX_ITEMS_PER_SECTION):
        chunk = lines[chunk_start : chunk_start + MAX_ITEMS_PER_SECTION]
        prefix = f"{header}\n" if first else ""
        blocks.append(_section(prefix + "\n".join(chunk)))
        first = False
    return blocks


def _format_new(tag: str, f: Finding) -> str:
    fix = f" → fix in `{f.fixed_in}`" if f.fixed_in else ""
    return f"• *{tag}* — `{f.cve_id}` ({f.severity}) `{f.package} {f.installed}`{fix}"


def _format_reminder(tag: str, f: Finding, first_seen: str) -> str:
    fix = f" → fix in `{f.fixed_in}`" if f.fixed_in else ""
    return (
        f"• *{tag}* — `{f.cve_id}` ({f.severity}) `{f.package} {f.installed}`{fix} "
        f"_first seen {first_seen[:10]}_"
    )


def _format_resolved(tag: str, v: dict[str, Any]) -> str:
    return f"• *{tag}* — `{v['cve_id']}` ({v['severity']}) `{v['package']} {v['installed']}`"


def post_slack(blocks: list[dict[str, Any]], channel: str) -> None:
    token = os.environ.get("SLACK_ACCESS_TOKEN")
    if not token:
        print("SLACK_ACCESS_TOKEN not set; skipping Slack post", file=sys.stderr)
        print(json.dumps(blocks, indent=2))
        return
    client = WebClient(token=token)
    client.chat_postMessage(
        channel=channel,
        blocks=blocks,
        text=":rotating_light: Apollo Agent vuln scan",
    )


# --- Main ---------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Nightly Docker Scout vulnerability scan for a Docker Hub repo.",
    )
    parser.add_argument(
        "--repo",
        required=True,
        help="Docker Hub repository to scan (e.g. montecarlodata/agent)",
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="S3 bucket name where scan state is persisted",
    )
    parser.add_argument(
        "--slack-channel",
        required=True,
        help="Slack channel to post deltas to (e.g. #vuln-alerts)",
    )
    parser.add_argument(
        "--tag",
        dest="tags",
        action="append",
        required=True,
        help="Tag to scan within --repo; pass once per tag (e.g. --tag latest-aws-proxied)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    repo: str = args.repo
    bucket: str = args.bucket
    slack_channel: str = args.slack_channel
    tags: list[str] = args.tags
    # Same bucket can host state for multiple repos without collision.
    s3_key = f"vuln-scan/{repo.split('/')[-1]}/state.json"

    now = datetime.now(timezone.utc).replace(microsecond=0)
    freshness_cutoff = now - timedelta(hours=FRESHNESS_GRACE_HOURS)

    print(f"Resolving tag info for {len(tags)} tags of {repo}…")
    tag_digests: dict[str, str] = {}
    skipped: list[tuple[str, str]] = []
    resolve_errors: list[tuple[str, str]] = []
    for tag in tags:
        try:
            info = get_tag_info(repo, tag)
            if info.last_pushed > freshness_cutoff:
                reason = f"pushed <{FRESHNESS_GRACE_HOURS}h ago, Scout may not be indexed yet"
                print(f"  {tag} -> SKIP ({reason})")
                skipped.append((tag, reason))
                continue
            tag_digests[tag] = info.digest
            print(f"  {tag} -> {info.digest}")
        except Exception as e:
            print(f"  ERROR resolving {tag}: {e}", file=sys.stderr)
            resolve_errors.append((f"{repo}:{tag}", str(e)))

    # Dedupe by digest so we don't scan the same bytes twice.
    digest_to_tags: dict[str, list[str]] = {}
    for tag, digest in tag_digests.items():
        digest_to_tags.setdefault(digest, []).append(tag)

    print(f"\nScanning {len(digest_to_tags)} unique digests…")
    scan_errors: list[tuple[str, str]] = list(resolve_errors)
    findings_by_digest: dict[str, list[Finding]] = {}
    for digest, tags in digest_to_tags.items():
        try:
            print(f"  scanning {digest} (tags: {', '.join(tags)})")
            findings_by_digest[digest] = scan_with_scout(repo, digest)
            print(f"    {len(findings_by_digest[digest])} findings")
        except subprocess.CalledProcessError as e:
            stderr = (e.stderr or "")[:500]
            err = f"scout failed (exit {e.returncode}): {stderr}"
            print(f"    ERROR: {err}", file=sys.stderr)
            scan_errors.append((digest, err))
        except Exception as e:
            print(f"    ERROR: {e}", file=sys.stderr)
            scan_errors.append((digest, str(e)))

    tag_findings = {
        tag: findings_by_digest[digest]
        for tag, digest in tag_digests.items()
        if digest in findings_by_digest
    }

    state = load_state(bucket, s3_key)
    print(f"\nLoaded state: {len(state.get('images', {}))} images previously tracked")

    summary, new_state = diff_and_update_state(
        tag_findings, tag_digests, state, now, skipped=skipped
    )

    print("\nSummary:")
    print(f"  new detailed (HIGH+): {len(summary.new_detailed)}")
    print(f"  reminders (HIGH+):    {len(summary.reminders)}")
    print(f"  resolved (HIGH+):     {len(summary.resolved_detailed)}")
    print(f"  skipped (fresh):      {len(summary.skipped)}")
    print(f"  scan errors:          {len(scan_errors)}")

    should_notify = bool(
        summary.new_detailed
        or summary.reminders
        or summary.resolved_detailed
        or scan_errors
    )
    if should_notify:
        blocks = build_slack_blocks(summary, scan_errors, tag_digests)
        post_slack(blocks, slack_channel)
        print("Posted to Slack.")
    else:
        print("No HIGH+ delta and no errors; skipping Slack post.")

    save_state(new_state, bucket, s3_key)
    print("State saved to S3.")
    return 1 if scan_errors else 0


if __name__ == "__main__":
    sys.exit(main())
