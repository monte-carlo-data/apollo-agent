"""Converts MCP SDK results to JSON-safe dicts and caps their size in the agent."""

import json
from typing import Any, Dict, List, Optional, Sequence

from mcp import types


def convert_call_result(result: types.CallToolResult) -> Dict[str, Any]:
    return {
        "content": [_dump(block) for block in result.content],
        "is_error": bool(result.isError),
        "structured_content": result.structuredContent,
        "truncated": False,
    }


def convert_tools(tools: Sequence[types.Tool]) -> List[Dict[str, Any]]:
    return [_dump(tool) for tool in tools]


def cap_call_result(result: Dict[str, Any], max_result_bytes: int) -> Dict[str, Any]:
    """
    Caps a converted `tools/call` result at `max_result_bytes` of JSON. Over the cap,
    `structured_content` is dropped (it can't be cut without breaking it), text
    blocks are cut from the end, other blocks that don't fit are dropped, and
    `truncated` is set.
    """
    if _size(result) <= max_result_bytes:
        return result
    result = {**result, "structured_content": None, "truncated": True}
    budget = max_result_bytes - _size({**result, "content": []})
    capped: List[Dict[str, Any]] = []
    for block in result["content"]:
        # each further block costs its own size plus a separating ", "
        cost = _size(block) + (2 if capped else 0)
        if cost <= budget:
            capped.append(block)
            budget -= cost
        elif block.get("type") == "text":
            cut = _cut_text_block(block, budget - (2 if capped else 0))
            if cut is not None:
                capped.append(cut)
                budget -= _size(cut) + (2 if len(capped) > 1 else 0)
    result["content"] = capped
    return result


def cap_tools_result(result: Dict[str, Any], max_result_bytes: int) -> Dict[str, Any]:
    """Caps a `tools/list` result by dropping tools from the end."""
    if _size(result) <= max_result_bytes:
        return result
    result = {**result, "truncated": True}
    budget = max_result_bytes - _size({**result, "tools": []})
    kept: List[Dict[str, Any]] = []
    for tool in result["tools"]:
        cost = _size(tool) + (2 if kept else 0)
        if cost > budget:
            break
        kept.append(tool)
        budget -= cost
    result["tools"] = kept
    return result


def _cut_text_block(block: Dict[str, Any], budget: int) -> Optional[Dict[str, Any]]:
    text = block.get("text", "")
    keep = min(len(text), max(0, budget))
    # JSON escaping can grow the text, so shrink until the block fits
    while keep > 0:
        cut = {**block, "text": text[:keep]}
        size = _size(cut)
        if size <= budget:
            return cut
        keep = min(keep - 1, keep * budget // size)
    return None


def _dump(model: Any) -> Dict[str, Any]:
    return model.model_dump(mode="json", by_alias=True, exclude_none=True)


def _size(value: Any) -> int:
    return len(json.dumps(value, default=str).encode("utf-8"))
