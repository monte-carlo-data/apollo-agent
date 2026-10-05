"""K2-1343 spike: print saved run records compactly."""

import json, sys
from pathlib import Path
from invoke import OUT

for pat in sys.argv[1:]:
    for f in sorted(OUT.glob(f"*{pat}*.json")):
        rec = json.loads(f.read_text())
        body = rec["body"]
        res = body.get("result", {})
        print(
            f"== {rec['name']}: wall={rec['client_wall_ms']} report={rec['report']} timings={res.get('timings')} is_error={res.get('is_error')} truncated={res.get('truncated')} bytes={len(json.dumps(res))} err={body.get('error_code')}:{str(body.get('error',''))[:300]}"
        )
        for c in res.get("content", []):
            print("   ", c.get("text", "")[:400].replace("\n", " "))
        for p in body.get("polls", []):
            pr = p["result"]
            print(
                "   poll waited",
                p["waited_s"],
                "op",
                pr.get("timings", {}).get("operation_ms"),
                [c.get("text", "")[:300] for c in pr.get("content", [])],
            )
