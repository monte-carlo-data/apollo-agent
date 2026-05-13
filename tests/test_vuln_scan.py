"""Tests for scripts/vuln_scan/scan.py.

Covers SARIF parsing, severity classification, purl parsing, and the diff
state-machine (bootstrap, new finding, reminder cadence, resolved finding).
No network or Docker calls are made.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from scripts.vuln_scan.scan import (
    REMINDER_DAYS,
    DiffSummary,
    Finding,
    FindingKey,
    _cvss_to_severity,
    _parse_purl,
    build_slack_blocks,
    diff_and_update_state,
    parse_sarif,
)


# --- _cvss_to_severity -------------------------------------------------------


@pytest.mark.parametrize(
    "score,expected",
    [
        (9.0, "CRITICAL"),
        (9.5, "CRITICAL"),
        (8.9, "HIGH"),
        (7.0, "HIGH"),
        (6.9, "MEDIUM"),
        (4.0, "MEDIUM"),
        (3.9, "LOW"),
        (0.1, "LOW"),
        (0.0, "UNKNOWN"),
    ],
)
def test_cvss_to_severity_boundaries(score: float, expected: str) -> None:
    assert _cvss_to_severity(score) == expected


# --- _parse_purl -------------------------------------------------------------


@pytest.mark.parametrize(
    "purl,expected",
    [
        ("pkg:pypi/urllib3@2.6.3", ("urllib3", "2.6.3")),
        ("pkg:deb/debian/libc6@2.41-12", ("libc6", "2.41-12")),
        ("pkg:golang/stdlib@go1.22.0", ("stdlib", "go1.22.0")),
        ("pkg:pypi/foo@1.0?qualifier=x", ("foo", "1.0")),
        ("pkg:pypi/no-version", ("pypi/no-version", "?")),
        ("not-a-purl", ("not-a-purl", "?")),
        ("", ("?", "?")),
    ],
)
def test_parse_purl(purl: str, expected: tuple[str, str]) -> None:
    assert _parse_purl(purl) == expected


# --- parse_sarif -------------------------------------------------------------


def _sarif_doc(rules: list[dict], results: list[dict]) -> str:
    return json.dumps(
        {
            "runs": [
                {
                    "tool": {"driver": {"rules": rules}},
                    "results": results,
                }
            ]
        }
    )


def test_parse_sarif_extracts_severity_from_cvss_score() -> None:
    sarif = _sarif_doc(
        rules=[
            {
                "id": "CVE-2026-44432",
                "properties": {
                    "cvss": {"v3": {"score": 8.9}},
                    "fixed-version": "2.6.4",
                },
            }
        ],
        results=[
            {
                "ruleId": "CVE-2026-44432",
                "locations": [
                    {
                        "physicalLocation": {
                            "artifactLocation": {"uri": "pkg:pypi/urllib3@2.6.3"}
                        }
                    }
                ],
            }
        ],
    )
    findings = parse_sarif(sarif)
    assert len(findings) == 1
    f = findings[0]
    assert f.cve_id == "CVE-2026-44432"
    assert f.package == "urllib3"
    assert f.installed == "2.6.3"
    assert f.severity == "HIGH"
    assert f.fixed_in == "2.6.4"


def test_parse_sarif_uses_security_severity_when_cvss_dict_missing() -> None:
    sarif = _sarif_doc(
        rules=[{"id": "CVE-X", "properties": {"security-severity": "9.1"}}],
        results=[
            {
                "ruleId": "CVE-X",
                "locations": [
                    {
                        "physicalLocation": {
                            "artifactLocation": {"uri": "pkg:deb/debian/libc6@2.41-12"}
                        }
                    }
                ],
            }
        ],
    )
    findings = parse_sarif(sarif)
    assert findings[0].severity == "CRITICAL"


def test_parse_sarif_falls_back_to_sarif_level() -> None:
    sarif = _sarif_doc(
        rules=[{"id": "CVE-Y", "properties": {}}],
        results=[
            {
                "ruleId": "CVE-Y",
                "level": "note",
                "locations": [
                    {
                        "physicalLocation": {
                            "artifactLocation": {"uri": "pkg:pypi/foo@1.0"}
                        }
                    }
                ],
            }
        ],
    )
    findings = parse_sarif(sarif)
    assert findings[0].severity == "LOW"


def test_parse_sarif_handles_empty_runs() -> None:
    assert parse_sarif(json.dumps({"runs": []})) == []


# --- diff_and_update_state ---------------------------------------------------


def _finding(
    cve: str, pkg: str = "urllib3", ver: str = "2.6.3", sev: str = "HIGH"
) -> Finding:
    return Finding(cve_id=cve, package=pkg, installed=ver, severity=sev, fixed_in=None)


def _now() -> datetime:
    return datetime(2026, 5, 13, 9, 0, 0, tzinfo=timezone.utc)


def test_bootstrap_first_run_treats_everything_as_new() -> None:
    findings = {
        "latest-cloudrun": [_finding("CVE-1", sev="HIGH"), _finding("CVE-2", sev="LOW")]
    }
    digests = {"latest-cloudrun": "sha256:abc"}
    summary, new_state = diff_and_update_state(findings, digests, state={}, now=_now())

    assert len(summary.new_detailed) == 1
    assert summary.new_detailed[0][1].cve_id == "CVE-1"
    assert summary.new_by_severity == {"HIGH": 1, "LOW": 1}
    assert summary.totals_by_severity == {"HIGH": 1, "LOW": 1}
    assert summary.reminders == []
    assert summary.resolved_detailed == []
    assert new_state["images"]["latest-cloudrun"]["digest"] == "sha256:abc"
    assert len(new_state["images"]["latest-cloudrun"]["vulns"]) == 2


def test_unchanged_high_below_reminder_threshold_is_silent() -> None:
    now = _now()
    one_day_ago = (now - timedelta(days=1)).isoformat()
    state = {
        "images": {
            "latest-cloudrun": {
                "digest": "sha256:abc",
                "vulns": [
                    {
                        "cve_id": "CVE-1",
                        "package": "urllib3",
                        "installed": "2.6.3",
                        "severity": "HIGH",
                        "fixed_in": None,
                        "first_seen": one_day_ago,
                        "last_notified": one_day_ago,
                    }
                ],
            }
        }
    }
    findings = {"latest-cloudrun": [_finding("CVE-1")]}
    digests = {"latest-cloudrun": "sha256:abc"}
    summary, _ = diff_and_update_state(findings, digests, state, now)

    assert summary.new_detailed == []
    assert summary.reminders == []
    assert summary.resolved_detailed == []


def test_high_open_for_reminder_threshold_re_notifies() -> None:
    now = _now()
    long_ago = (now - timedelta(days=REMINDER_DAYS + 1)).isoformat()
    state = {
        "images": {
            "latest-cloudrun": {
                "digest": "sha256:abc",
                "vulns": [
                    {
                        "cve_id": "CVE-1",
                        "package": "urllib3",
                        "installed": "2.6.3",
                        "severity": "HIGH",
                        "fixed_in": None,
                        "first_seen": long_ago,
                        "last_notified": long_ago,
                    }
                ],
            }
        }
    }
    findings = {"latest-cloudrun": [_finding("CVE-1")]}
    digests = {"latest-cloudrun": "sha256:abc"}
    summary, new_state = diff_and_update_state(findings, digests, state, now)

    assert len(summary.reminders) == 1
    tag, finding, first_seen = summary.reminders[0]
    assert tag == "latest-cloudrun"
    assert finding.cve_id == "CVE-1"
    assert first_seen == long_ago
    # last_notified should bump forward
    updated = new_state["images"]["latest-cloudrun"]["vulns"][0]
    assert updated["last_notified"] == now.isoformat()
    assert updated["first_seen"] == long_ago


def test_resolved_high_appears_in_resolved_list() -> None:
    now = _now()
    state = {
        "images": {
            "latest-cloudrun": {
                "digest": "sha256:abc",
                "vulns": [
                    {
                        "cve_id": "CVE-RESOLVED",
                        "package": "urllib3",
                        "installed": "2.6.3",
                        "severity": "HIGH",
                        "fixed_in": "2.6.4",
                        "first_seen": now.isoformat(),
                        "last_notified": now.isoformat(),
                    }
                ],
            }
        }
    }
    findings: dict[str, list[Finding]] = {"latest-cloudrun": []}
    digests = {"latest-cloudrun": "sha256:abc"}
    summary, new_state = diff_and_update_state(findings, digests, state, now)

    assert len(summary.resolved_detailed) == 1
    tag, vuln = summary.resolved_detailed[0]
    assert tag == "latest-cloudrun"
    assert vuln["cve_id"] == "CVE-RESOLVED"
    assert new_state["images"]["latest-cloudrun"]["vulns"] == []


def test_skipped_tag_preserves_previous_state_verbatim() -> None:
    """If a tag is skipped (e.g. pushed too recently), its previous state
    record is copied into the new state unchanged so the next run produces a
    correct diff and no false-resolved deltas appear in the meantime."""
    now = _now()
    prev_image = {
        "digest": "sha256:old",
        "vulns": [
            {
                "cve_id": "CVE-99",
                "package": "urllib3",
                "installed": "2.6.3",
                "severity": "HIGH",
                "fixed_in": None,
                "first_seen": (now - timedelta(days=2)).isoformat(),
                "last_notified": (now - timedelta(days=2)).isoformat(),
            }
        ],
    }
    state = {"images": {"latest-cloudrun": prev_image}}
    summary, new_state = diff_and_update_state(
        tag_findings={},
        tag_digests={},
        state=state,
        now=now,
        skipped=[("latest-cloudrun", "pushed <6h ago")],
    )
    # Skipped image's state is preserved verbatim.
    assert new_state["images"]["latest-cloudrun"] == prev_image
    # No deltas reported for the skipped tag.
    assert summary.new_detailed == []
    assert summary.resolved_detailed == []
    assert summary.reminders == []
    # The skip itself is recorded on the summary.
    assert summary.skipped == [("latest-cloudrun", "pushed <6h ago")]


def test_skipped_tag_with_no_previous_state_is_omitted_from_new_state() -> None:
    """A skipped tag we've never seen before just isn't in the new state."""
    now = _now()
    summary, new_state = diff_and_update_state(
        tag_findings={},
        tag_digests={},
        state={"images": {}},
        now=now,
        skipped=[("latest-newthing", "pushed <6h ago")],
    )
    assert "latest-newthing" not in new_state["images"]
    assert summary.skipped == [("latest-newthing", "pushed <6h ago")]


def test_low_severity_resolved_is_not_in_detailed_resolved() -> None:
    """Only HIGH/CRITICAL are surfaced as resolved; lower severities go silent."""
    now = _now()
    state = {
        "images": {
            "latest-cloudrun": {
                "digest": "sha256:abc",
                "vulns": [
                    {
                        "cve_id": "CVE-LOW",
                        "package": "foo",
                        "installed": "1.0",
                        "severity": "LOW",
                        "fixed_in": None,
                        "first_seen": now.isoformat(),
                        "last_notified": now.isoformat(),
                    }
                ],
            }
        }
    }
    findings: dict[str, list[Finding]] = {"latest-cloudrun": []}
    digests = {"latest-cloudrun": "sha256:abc"}
    summary, _ = diff_and_update_state(findings, digests, state, now)
    assert summary.resolved_detailed == []


# --- build_slack_blocks ------------------------------------------------------


def test_slack_blocks_include_header_totals_and_scanned() -> None:
    summary = DiffSummary(
        new_detailed=[("latest-cloudrun", _finding("CVE-1"))],
        totals_by_severity={"HIGH": 1, "MEDIUM": 12},
        new_by_severity={"HIGH": 1, "MEDIUM": 3},
    )
    blocks = build_slack_blocks(
        summary, scan_errors=[], tag_digests={"latest-cloudrun": "sha256:abc1234567890"}
    )
    # First block is the header.
    assert blocks[0]["type"] == "header"
    # Totals + scanned context blocks appear at the bottom.
    context_texts = [
        el["text"]
        for b in blocks
        if b["type"] == "context"
        for el in b.get("elements", [])
    ]
    assert any("Totals:" in t for t in context_texts)
    assert any("Scanned:" in t and "latest-cloudrun" in t for t in context_texts)


def test_slack_blocks_chunk_long_new_lists() -> None:
    """Section text > 3000 chars would be rejected by Slack; we split."""
    many = [("latest-cloudrun", _finding(f"CVE-{i}")) for i in range(85)]
    summary = DiffSummary(
        new_detailed=many,
        totals_by_severity={"HIGH": 85},
        new_by_severity={"HIGH": 85},
    )
    blocks = build_slack_blocks(
        summary, scan_errors=[], tag_digests={"latest-cloudrun": "sha256:abc1234567890"}
    )
    section_blocks = [b for b in blocks if b["type"] == "section"]
    # 85 items / 40 per section = 3 section blocks for the new-detailed group.
    assert len(section_blocks) >= 3


def test_slack_blocks_include_skipped_context_line() -> None:
    summary = DiffSummary(
        skipped=[("latest-cloudrun", "pushed <6h ago")],
        totals_by_severity={"HIGH": 1},
        new_by_severity={"HIGH": 1},
        new_detailed=[("latest-azure", _finding("CVE-X"))],
    )
    blocks = build_slack_blocks(
        summary, scan_errors=[], tag_digests={"latest-azure": "sha256:abc1234567890"}
    )
    context_texts = [
        el["text"]
        for b in blocks
        if b["type"] == "context"
        for el in b.get("elements", [])
    ]
    assert any("Skipped:" in t and "latest-cloudrun" in t for t in context_texts)


def test_slack_blocks_show_scan_errors_section() -> None:
    summary = DiffSummary()
    blocks = build_slack_blocks(
        summary,
        scan_errors=[("sha256:abc", "scout failed: timeout")],
        tag_digests={"latest-cloudrun": "sha256:abc1234567890"},
    )
    sections = [b for b in blocks if b["type"] == "section"]
    assert any(":x:" in b["text"]["text"] for b in sections)
    assert any("Scan errors" in b["text"]["text"] for b in sections)


# --- FindingKey -------------------------------------------------------------


def test_finding_key_is_hashable_and_value_based() -> None:
    a = FindingKey("CVE-1", "urllib3", "2.6.3")
    b = FindingKey("CVE-1", "urllib3", "2.6.3")
    c = FindingKey("CVE-1", "urllib3", "2.6.4")
    assert a == b
    assert hash(a) == hash(b)
    assert a != c
    assert {a, b} == {a}
