import json
from typing import Any
from unittest import TestCase

from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient


class _StubDbProxyClient(BaseDbProxyClient):
    """Concrete subclass: BaseDbProxyClient is ABC only by inheritance, no abstract members."""

    def __init__(self) -> None:
        super().__init__(connection_type="stub-db")

    @property
    def wrapped_client(self) -> Any:
        return None


class TestProcessResult(TestCase):
    def setUp(self) -> None:
        self.client = _StubDbProxyClient()

    def test_cursor_result_description_and_rows_are_processed(self):
        """The DB-API cursor shape keeps its long-standing behavior: 7-tuples in
        `description` are re-emitted as 7-element lists and rows are serialized."""
        cursor_result = {
            "description": [("ID", "NUMBER", None, None, 38, 0, False)],
            "all_results": [[1], [2]],
            "rowcount": 2,
        }
        processed = self.client.process_result(cursor_result)
        self.assertEqual(
            processed["description"], [["ID", "NUMBER", None, None, 38, 0, False]]
        )
        self.assertEqual(processed["all_results"], [[1], [2]])

    def test_cursor_result_none_description_becomes_empty_list(self):
        """Cursor results with no result set surface `description: []` (legacy
        behavior some callers rely on when iterating the description)."""
        processed = self.client.process_result(
            {"description": None, "all_results": None}
        )
        self.assertEqual(processed["description"], [])
        self.assertEqual(processed["all_results"], [])

    def test_rest_passthrough_body_with_description_string_is_untouched(self):
        """A REST-passthrough result (e.g. SFDC `ssot_get` of a Data 360 Retriever
        detail, YET-2410) whose resource carries a human-readable `description`
        STRING must pass through verbatim. The old code iterated the string and
        indexed each character (`col[1]`), raising
        `IndexError: string index out of range` for every such body."""
        rest_body = {
            "name": "WebRetrievalAction",
            "description": "Web search retriever retrieves search results from the internet.",
            "activeConfiguration": {
                "name": "WebRetrievalActionVersion",
                "isActive": True,
            },
        }
        expected = json.loads(json.dumps(rest_body))
        processed = self.client.process_result(rest_body)
        self.assertEqual(processed, expected)

    def test_non_dbapi_description_list_is_untouched(self):
        """A `description` list whose elements are not 7+-element sequences is not
        a DB-API description — leave it alone rather than raising IndexError."""
        rest_body = {"description": ["short", "strings"]}
        processed = self.client.process_result(rest_body)
        self.assertEqual(processed["description"], ["short", "strings"])

    def test_rest_passthrough_all_results_string_is_untouched(self):
        """Same guard for `all_results`: only list/tuple containers are treated as
        cursor rows."""
        rest_body = {"all_results": "not-a-row-container"}
        processed = self.client.process_result(rest_body)
        self.assertEqual(processed["all_results"], "not-a-row-container")
