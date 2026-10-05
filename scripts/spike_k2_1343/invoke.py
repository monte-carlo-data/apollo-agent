"""K2-1343 spike: invoke the harness Lambda and record results under .work/."""

import base64
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

import boto3
from botocore.config import Config

from provision import ACCT, NARROW_ROLE, REGION, HARNESS

OUT = Path(__file__).resolve().parents[2] / ".work/k2-1343-spike-aws-mcp-sigv4/runs"
LOG_GROUP = "/k2-1343-spike/app"

# no retries: a retried read timeout would invoke the harness twice
_lam = boto3.Session(profile_name="dev").client(
    "lambda",
    region_name=REGION,
    config=Config(read_timeout=310, retries={"max_attempts": 1}),
)


def aws_server(
    region: str = REGION, role: str = f"arn:aws:iam::{ACCT}:role/{NARROW_ROLE}"
) -> Dict[str, Any]:
    return {
        "url": f"https://aws-mcp.{region}.api.aws/mcp",
        "transport": "streamable_http",
        "auth": {"type": "aws_sigv4", "region": region, "assumable_role": role},
    }


def run_script(
    code: str, server: Optional[Dict] = None, **extra: Any
) -> Dict[str, Any]:
    return {
        "action": "mcp",
        "server": server or aws_server(),
        "operation": "call_tool",
        "tool": "aws___run_script",
        "arguments": {"code": code},
        "limits": {"timeout_seconds": 60, "max_result_bytes": 200_000},
        "poll_tasks": True,
        **extra,
    }


def invoke(name: str, event: Dict[str, Any]) -> Dict[str, Any]:
    start = time.perf_counter()
    resp = _lam.invoke(
        FunctionName=HARNESS, Payload=json.dumps(event).encode(), LogType="Tail"
    )
    wall_ms = int((time.perf_counter() - start) * 1000)
    tail = base64.b64decode(resp.get("LogResult", "")).decode(errors="replace")
    report = {}
    for key in ("Init Duration", "Duration", "Billed Duration", "Max Memory Used"):
        m = re.search(rf"\t{key}: ([\d.]+)", tail)
        if m:
            report[key] = float(m.group(1))
    body = json.loads(resp["Payload"].read())
    record = {
        "name": name,
        "event": event,
        "client_wall_ms": wall_ms,
        "report": report,
        "body": body,
    }
    if resp.get("FunctionError"):
        record["function_error"] = resp["FunctionError"]
        record["log_tail"] = tail[-3000:]
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"{int(time.time())}-{name}.json").write_text(
        json.dumps(record, indent=1, default=str)
    )
    return record


def summarize(record: Dict[str, Any], text_chars: int = 600) -> None:
    body = record["body"]
    res = body.get("result", {})
    texts = [
        c.get("text", "") for c in res.get("content", []) if c.get("type") == "text"
    ]
    print(
        f"== {record['name']}: wall={record['client_wall_ms']}ms report={record['report']} "
        f"harness={body.get('harness')} timings={res.get('timings')} is_error={res.get('is_error')} "
        f"truncated={res.get('truncated')} polls={len(body.get('polls', []))}"
    )
    if "error" in body:
        print("   ERROR", body.get("error_code"), body["error"][:text_chars])
    if record.get("function_error"):
        print("   FUNCTION ERROR", record["log_tail"][-text_chars:])
    for t in texts:
        print("   ", t[:text_chars].replace("\n", " "))
    if res.get("structured_content"):
        print("   structured:", json.dumps(res["structured_content"])[:text_chars])


if __name__ == "__main__":
    summarize(invoke(sys.argv[1], json.loads(sys.argv[2])))
