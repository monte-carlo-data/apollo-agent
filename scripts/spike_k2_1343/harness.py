"""
K2-1343 spike harness Lambda (throwaway).

Runs with an execution role that mirrors the collection agent's, and drives
apollo/integrations/mcp/core.py (vendored as ``mcp_core``) exactly as the agent
would: assume the narrow role per call, open an MCP session, call one tool.

Event shapes:
  {"action": "mcp", "server": {...}, "operation": "list_tools"|"call_tool",
   "tool": "...", "arguments": {...}, "limits": {...},
   "secret_id": "<optional ASM id holding header_value>", "poll_tasks": true}
  {"action": "exec_controls", ...}   positive controls using the execution role
"""

import json
import os
import time
from typing import Any, Dict, List, Optional

import boto3

_INIT_START = time.perf_counter()
import mcp_core  # type: ignore[import-not-found]  # noqa: E402  vendored at build; import cost is part of cold start

_IMPORT_MS = int((time.perf_counter() - _INIT_START) * 1000)
_COLD = True


def _first_json_text(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if isinstance(result.get("structured_content"), dict):
        return result["structured_content"]
    for block in result.get("content") or []:
        if block.get("type") == "text":
            try:
                parsed = json.loads(block["text"])
            except (ValueError, TypeError):
                continue
            if isinstance(parsed, dict):
                return parsed
    return None


def _find_task(payload: Any) -> Optional[str]:
    """Return a task id if the response says the script is still working."""
    if isinstance(payload, dict):
        if payload.get("task_id") and str(payload.get("status", "")).lower() in (
            "working",
            "running",
            "pending",
        ):
            return payload["task_id"]
        for value in payload.values():
            found = _find_task(value)
            if found:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = _find_task(value)
            if found:
                return found
    return None


def _mcp(event: Dict[str, Any]) -> Dict[str, Any]:
    server = event["server"]
    secrets: Dict[str, Any] = {}
    if event.get("secret_id"):
        secret = boto3.client("secretsmanager").get_secret_value(
            SecretId=event["secret_id"]
        )
        secrets = json.loads(secret["SecretString"])
    limits = event.get("limits") or {}
    timeout = float(limits.get("timeout_seconds", 20))
    max_bytes = int(limits.get("max_result_bytes", 200_000))

    resolved = mcp_core.resolve_auth(server.get("auth") or {}, secrets)
    mcp_core.check_server_url(
        server["url"],
        list(mcp_core.DEFAULT_ALLOWED_HOST_PATTERNS) + event.get("extra_hosts", []),
    )
    result = mcp_core.run_mcp_operation(
        server["url"],
        resolved,
        event["operation"],
        tool=event.get("tool"),
        arguments=event.get("arguments"),
        timeout_seconds=timeout,
        max_result_bytes=max_bytes,
    )

    polls: List[Dict[str, Any]] = []
    task_id = _find_task(_first_json_text(result)) if event.get("poll_tasks") else None
    iteration = 0
    while task_id and iteration < 30:
        iteration += 1
        payload = (
            _first_json_text(result if iteration == 1 else polls[-1]["result"]) or {}
        )
        wait = float(_dig(payload, "recommended_wait_seconds") or 1)
        time.sleep(min(wait, 10))
        poll_auth = mcp_core.resolve_auth(server.get("auth") or {}, secrets)
        polled = mcp_core.run_mcp_operation(
            server["url"],
            poll_auth,
            "call_tool",
            tool="aws___get_tasks",
            arguments={"task_ids": [task_id], "poll_iteration": iteration},
            timeout_seconds=timeout,
            max_result_bytes=max_bytes,
        )
        polls.append({"waited_s": wait, "result": polled})
        task_id = _find_task(_first_json_text(polled))
    return {"result": result, "polls": polls}


def _dig(payload: Any, key: str) -> Any:
    if isinstance(payload, dict):
        if key in payload:
            return payload[key]
        for value in payload.values():
            found = _dig(value, key)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = _dig(value, key)
            if found is not None:
                return found
    return None


def _exec_controls(event: Dict[str, Any]) -> Dict[str, Any]:
    """Prove the execution role really can do what the script must not."""
    out: Dict[str, Any] = {}
    try:
        obj = boto3.client("s3").get_object(Bucket=event["bucket"], Key=event["key"])
        out["s3_get_object"] = f"ok ({len(obj['Body'].read())} bytes)"
    except Exception as exc:  # noqa: BLE001
        out["s3_get_object"] = f"error: {exc}"
    try:
        boto3.client("secretsmanager").get_secret_value(SecretId=event["secret_id"])
        out["secret_get"] = "ok"
    except Exception as exc:  # noqa: BLE001
        out["secret_get"] = f"error: {exc}"
    try:
        boto3.client("sts").assume_role(
            RoleArn=event["decoy_role"], RoleSessionName="k2-1343-control"
        )
        out["assume_decoy"] = "ok"
    except Exception as exc:  # noqa: BLE001
        out["assume_decoy"] = f"error: {exc}"
    return out


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    global _COLD
    cold, _COLD = _COLD, False
    start = time.perf_counter()
    try:
        if event.get("action") == "exec_controls":
            body: Dict[str, Any] = _exec_controls(event)
        else:
            body = _mcp(event)
    except mcp_core.McpClientError as exc:
        body = {"error_code": exc.code, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        body = {"error_code": "server_error", "error": f"{type(exc).__name__}: {exc}"}
    body["harness"] = {
        "cold": cold,
        "import_ms": _IMPORT_MS if cold else None,
        "handler_ms": int((time.perf_counter() - start) * 1000),
        "memory_mb": os.getenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE"),
    }
    return body
