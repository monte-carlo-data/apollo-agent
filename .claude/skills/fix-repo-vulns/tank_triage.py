#!/usr/bin/env python3
"""tank_triage.py — thin wrapper around mc-security's eval-gated Tank judge
(vuln-mgmt-triage/source/tank_disposition.dispose).

Reads ONE finding as JSON on stdin, prints its Disposition as JSON on stdout.
Side-effect-free (no scanner / Linear / GitHub writes) — it only asks Tank's
judge "real vuln or false positive?". The fix-repo-vulns skill calls this to
triage each finding before fixing, so it stops "fixing"/documenting false
positives. It NEVER suppresses anything; suppression stays human-gated.

Fails SAFE: any import/parse/model error -> {"parse_ok": false}, which the skill
treats as "no disposition" (fix/document as usual), never as a false positive.

Env:
  TANK_SRC          path to mc-security vuln-mgmt-triage/source (has tank_disposition.py)
  ANTHROPIC_API_KEY required by dispose()
Deps: pip install anthropic

In  (stdin JSON): {"source","finding_type"|"kind","title"|"summary","context"|"detail"}
Out (stdout JSON): {"disposition","reachability","rationale","parse_ok"} | {"parse_ok":false,"error":...}
"""
import json
import os
import sys


def main() -> None:
    src = os.environ.get("TANK_SRC", "").strip()
    if src and src not in sys.path:
        sys.path.insert(0, src)
    try:
        from tank_disposition import dispose, Finding  # type: ignore
    except Exception as e:  # import failure -> fail safe
        print(json.dumps({"parse_ok": False, "error": f"tank_disposition import failed: {e}"}))
        return
    try:
        f = json.loads(sys.stdin.read() or "{}")
        result = dispose(Finding(
            source=str(f.get("source", "") or ""),
            finding_type=str(f.get("finding_type") or f.get("kind") or ""),
            title=str(f.get("title") or f.get("summary") or ""),
            context=str(f.get("context") or f.get("detail") or ""),
        ))
        print(json.dumps({
            "disposition": result.disposition,
            "reachability": result.reachability,
            "rationale": result.rationale,
            "parse_ok": result.parse_ok,
        }))
    except Exception as e:  # any runtime error -> fail safe
        print(json.dumps({"parse_ok": False, "error": str(e)}))


if __name__ == "__main__":
    main()
