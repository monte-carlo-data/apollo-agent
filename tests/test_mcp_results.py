import json
from unittest import TestCase

import pytest

pytest.importorskip("mcp")

from mcp import types  # noqa: E402

from apollo.integrations.mcp.results import (  # noqa: E402
    cap_call_result,
    cap_tools_result,
    convert_call_result,
    convert_tools,
)


def _size(value) -> int:
    return len(json.dumps(value).encode("utf-8"))


class TestConvert(TestCase):
    def test_text_structured_and_resource_content(self):
        result = types.CallToolResult(
            content=[
                types.TextContent(type="text", text="hello"),
                types.ImageContent(type="image", data="aGk=", mimeType="image/png"),
                types.EmbeddedResource(
                    type="resource",
                    resource=types.TextResourceContents(
                        uri="file:///x.txt", text="inside", mimeType="text/plain"
                    ),
                ),
                types.ResourceLink(type="resource_link", uri="file:///y", name="y"),
            ],
            structuredContent={"status": "success", "api_calls": []},
            isError=False,
        )

        converted = convert_call_result(result)

        self.assertEqual(
            [
                {"type": "text", "text": "hello"},
                {"type": "image", "data": "aGk=", "mimeType": "image/png"},
                {
                    "type": "resource",
                    "resource": {
                        "uri": "file:///x.txt",
                        "mimeType": "text/plain",
                        "text": "inside",
                    },
                },
                {"type": "resource_link", "uri": "file:///y", "name": "y"},
            ],
            converted["content"],
        )
        self.assertEqual(
            {"status": "success", "api_calls": []}, converted["structured_content"]
        )
        self.assertFalse(converted["is_error"])
        self.assertFalse(converted["truncated"])
        json.dumps(converted)  # JSON-safe

    def test_error_result(self):
        result = types.CallToolResult(
            content=[types.TextContent(type="text", text="bad")], isError=True
        )
        converted = convert_call_result(result)
        self.assertTrue(converted["is_error"])
        self.assertIsNone(converted["structured_content"])

    def test_tools(self):
        tools = [
            types.Tool(
                name="aws___run_script",
                description="runs",
                inputSchema={"type": "object"},
                annotations=types.ToolAnnotations(readOnlyHint=False),
            )
        ]
        self.assertEqual(
            [
                {
                    "name": "aws___run_script",
                    "description": "runs",
                    "inputSchema": {"type": "object"},
                    "annotations": {"readOnlyHint": False},
                }
            ],
            convert_tools(tools),
        )


class TestCap(TestCase):
    def _call_result(self, *content, structured=None):
        return {
            "content": list(content),
            "is_error": False,
            "structured_content": structured,
            "truncated": False,
        }

    def test_small_result_untouched(self):
        result = self._call_result({"type": "text", "text": "hi"}, structured={"a": 1})
        self.assertEqual(result, cap_call_result(dict(result), 10_000))

    def test_text_cut_to_fit(self):
        text = 'line "quoted"\n' * 2000 + "é" * 2000
        result = self._call_result(
            {"type": "text", "text": text}, structured={"x": text}
        )

        capped = cap_call_result(result, 5_000)

        self.assertTrue(capped["truncated"])
        self.assertIsNone(capped["structured_content"])
        self.assertLessEqual(_size(capped), 5_000)
        kept = capped["content"][0]["text"]
        self.assertTrue(text.startswith(kept))
        self.assertGreater(len(kept), 1_000)

    def test_blocks_after_budget_dropped(self):
        result = self._call_result(
            {"type": "text", "text": "a" * 100},
            {"type": "image", "data": "x" * 10_000, "mimeType": "image/png"},
            {"type": "text", "text": "b" * 100},
        )

        capped = cap_call_result(result, 1_000)

        self.assertTrue(capped["truncated"])
        self.assertEqual(["a" * 100, "b" * 100], [b["text"] for b in capped["content"]])
        self.assertLessEqual(_size(capped), 1_000)

    def test_tools_dropped_from_the_end(self):
        tools = [{"name": f"t{i}", "description": "d" * 100} for i in range(50)]

        capped = cap_tools_result({"tools": tools, "truncated": False}, 1_000)

        self.assertTrue(capped["truncated"])
        self.assertLessEqual(_size(capped), 1_000)
        self.assertEqual(tools[: len(capped["tools"])], capped["tools"])
