"""
K2-1343 spike: run many run_script probes in ONE warm MCP session (first call
pays the ~9 s sandbox start, later calls ~1.3 s). Signs with the narrow role,
assumed from the operator's SSO session (the role trusts it for the spike).

  AWS_PROFILE=dev python session_runner.py <suite> [region] [role_arn]
"""

import json
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import anyio
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.types import TextContent

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from apollo.integrations.mcp.core import resolve_auth  # noqa: E402

from invoke import OUT  # noqa: E402
from provision import (
    ACCT,
    BUCKET,
    BUCKET_KEY,
    DECOY_ROLE,
    NARROW_ROLE,
    SECRET,
)  # noqa: E402

LG = "/k2-1343-spike/app"


def boto(
    service: str, op: str, params: Optional[Dict] = None, region: Optional[str] = None
) -> str:
    region_arg = f', region_name="{region}"' if region else ""
    return (
        f'r = await call_boto3(service_name="{service}", operation_name="{op}"{region_arg}, '
        f"params={json.dumps(params or {})})\nresult = r\nresult"
    )


SUITES = {
    "containment": [
        ("identity", boto("sts", "GetCallerIdentity")),
        (
            "logs_allowed",
            boto("logs", "FilterLogEvents", {"logGroupName": LG, "limit": 1}),
        ),
        (
            "logs_other_group",
            boto(
                "logs",
                "FilterLogEvents",
                {"logGroupName": "/aws/lambda/mcd-agent-service-080291fd", "limit": 1},
                region="us-west-2",
            ),
        ),
        (
            "assume_decoy",
            boto(
                "sts",
                "AssumeRole",
                {
                    "RoleArn": f"arn:aws:iam::{ACCT}:role/{DECOY_ROLE}",
                    "RoleSessionName": "x",
                },
            ),
        ),
        (
            "s3_agent_bucket",
            boto("s3", "GetObject", {"Bucket": BUCKET, "Key": BUCKET_KEY}),
        ),
        ("s3_list_bucket", boto("s3", "ListObjectsV2", {"Bucket": BUCKET})),
        ("secret_get", boto("secretsmanager", "GetSecretValue", {"SecretId": SECRET})),
        ("logs_describe_groups", boto("logs", "DescribeLogGroups", {"limit": 5})),
    ],
    "sandbox": [
        (
            "import_os_environ",
            "import os\nresult = {k: (v[:6] + '...') for k, v in os.environ.items()}\nresult",
        ),
        ("open_file", "result = open('/etc/passwd').read()[:200]\nresult"),
        (
            "socket",
            "import socket\ns = socket.create_connection(('example.com', 80), timeout=3)\nresult = 'connected'\nresult",
        ),
        (
            "urllib",
            "import urllib.request\nresult = urllib.request.urlopen('https://example.com', timeout=3).status\nresult",
        ),
        (
            "subprocess",
            "import subprocess\nresult = subprocess.run(['id'], capture_output=True).stdout.decode()\nresult",
        ),
        (
            "builtins_list",
            "result = sorted(k for k in __builtins__) if isinstance(__builtins__, dict) else sorted(dir(__builtins__))\nresult",
        ),
        ("globals_list", "result = sorted(globals().keys())\nresult"),
        (
            "call_boto3_repr",
            "result = {'repr': repr(call_boto3), 'type': str(type(call_boto3))}\nresult",
        ),
        (
            "class_walk",
            "result = [c.__name__ for c in ().__class__.__base__.__subclasses__()][:40]\nresult",
        ),
    ],
    "aws_mcp": [
        (
            "aws_mcp_help",
            "import aws_mcp\nimport io, contextlib\nb = io.StringIO()\nwith contextlib.redirect_stdout(b):\n    help(aws_mcp)\nresult = b.getvalue()[:6000]\nresult",
        ),
        (
            "aws_mcp_dir",
            "import aws_mcp\nresult = [n for n in dir(aws_mcp) if not n.startswith('_')]\nresult",
        ),
        (
            "aws_mcp_search_logs",
            "import aws_mcp\nresult = await aws_mcp.search_functions('search cloudwatch logs for errors')\nresult",
        ),
        (
            "aws_mcp_search_secrets",
            "import aws_mcp\nresult = await aws_mcp.search_functions('read secrets manager secret value')\nresult",
        ),
    ],
}


def ms(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)


async def run_suite(
    suite: str,
    region: str,
    role: str,
    extra: Optional[List[Tuple[str, str]]] = None,
) -> List[Dict[str, Any]]:
    resolved = resolve_auth(
        {"type": "aws_sigv4", "region": region, "assumable_role": role}, {}
    )
    rows = []
    async with streamablehttp_client(
        f"https://aws-mcp.{region}.api.aws/mcp",
        auth=resolved.auth,
        timeout=120,
        sse_read_timeout=120,
    ) as (rd, wr, _):
        async with ClientSession(rd, wr) as session:
            await session.initialize()
            for name, code in extra or SUITES[suite]:
                start = time.perf_counter()
                try:
                    res = await session.call_tool(
                        "aws___run_script",
                        {"code": code},
                        read_timeout_seconds=timedelta(seconds=120),
                        meta=resolved.meta,
                    )
                    text = "".join(
                        c.text for c in res.content if isinstance(c, TextContent)
                    )
                    try:
                        payload = json.loads(text)
                    except ValueError:
                        payload = {"raw": text}
                    row = {
                        "name": name,
                        "ms": ms(start),
                        "is_error": res.isError,
                        "payload": payload,
                    }
                except Exception as exc:  # noqa: BLE001
                    row = {
                        "name": name,
                        "ms": ms(start),
                        "exception": f"{type(exc).__name__}: {exc}",
                    }
                rows.append(row)
                p = row.get("payload", {})
                print(
                    f"== {name} [{row['ms']}ms] is_error={row.get('is_error')} status={p.get('status')} "
                    f"api_calls={json.dumps(p.get('api_calls'))[:300]}"
                )
                detail = (
                    p.get("return_value")
                    if p.get("status") == "success"
                    else (
                        p.get("error")
                        or p.get("stderr")
                        or p.get("raw")
                        or row.get("exception")
                    )
                )
                print("   ", json.dumps(detail, default=str)[:700])
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"{int(time.time())}-session-{suite}-{region}.json").write_text(
        json.dumps(rows, indent=1, default=str)
    )
    return rows


if __name__ == "__main__":
    suite = sys.argv[1]
    region = sys.argv[2] if len(sys.argv) > 2 else "us-west-2"
    role = (
        sys.argv[3] if len(sys.argv) > 3 else f"arn:aws:iam::{ACCT}:role/{NARROW_ROLE}"
    )
    anyio.run(run_suite, suite, region, role)
