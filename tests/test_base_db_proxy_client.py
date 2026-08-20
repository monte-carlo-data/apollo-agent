import json
from typing import Any
from unittest import TestCase

from psycopg2.extensions import Column

from apollo.common.agent.serde import AgentSerializer
from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient


class _StubDbProxyClient(BaseDbProxyClient):
    """Concrete subclass: BaseDbProxyClient is ABC only by inheritance, no abstract members."""

    def __init__(self) -> None:
        super().__init__(connection_type="stub-db")

    @property
    def wrapped_client(self) -> Any:
        return None


class TestProcessResultAcceptsDriverColumnObjects(TestCase):
    """Guard the base class against being narrowed to tuple-only column descriptors.

    `process_result` reaches every DB client, and several drivers return `description`
    entries that implement only the sequence protocol rather than subclassing `tuple`:
    `psycopg2.extensions.Column` (Postgres and, by inheritance, Redshift) and
    `oracledb.FetchInfo`. Positional indexing (`col[0]`…`col[6]`) is what makes those work,
    and an `isinstance(col, (list, tuple))` gate here would silently skip serialization for
    all three integrations — the raw driver objects would then reach `json.dumps` and raise
    `TypeError: Object of type Column is not JSON serializable`.

    A REST-passthrough body carrying a top-level `description` string is handled by the
    client that can actually recognize one — see
    `SalesforceDataCloudProxyClient.process_result` — precisely so this shared path does not
    have to model every driver's descriptor type.
    """

    def setUp(self) -> None:
        self.client = _StubDbProxyClient()

    def test_psycopg2_column_descriptors_are_serialized(self):
        """A real psycopg2 `Column` is not a tuple but is indexable — it must still be
        converted to a JSON-serializable 7-element list."""
        column = Column(
            name="id",
            type_code=23,
            display_size=None,
            internal_size=4,
            precision=None,
            scale=None,
            null_ok=None,
        )
        self.assertFalse(isinstance(column, (list, tuple)), "precondition: not a tuple")

        processed = self.client.process_result(
            {"description": [column], "all_results": []}
        )

        self.assertEqual(
            processed["description"], [["id", 23, None, 4, None, None, None]]
        )
        # The point of the transform: the result must survive response encoding.
        json.dumps(processed, cls=AgentSerializer)

    def test_plain_tuple_descriptors_are_serialized(self):
        """The common shape (Snowflake's NamedTuple, pyodbc/trino tuples) is unchanged."""
        processed = self.client.process_result(
            {
                "description": [("ID", "NUMBER", None, None, 38, 0, False)],
                "all_results": [[1]],
            }
        )
        self.assertEqual(
            processed["description"], [["ID", "NUMBER", None, None, 38, 0, False]]
        )
        self.assertEqual(processed["all_results"], [[1]])

    def test_none_description_and_rows_become_empty_lists(self):
        """The row-less cursor shape callers iterate."""
        processed = self.client.process_result(
            {"description": None, "all_results": None}
        )
        self.assertEqual(processed["description"], [])
        self.assertEqual(processed["all_results"], [])
