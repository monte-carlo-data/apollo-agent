from unittest import TestCase
from unittest.mock import Mock

import pyarrow
from databricks import sql
from databricks.sql import result_set, utils
from databricks.sql.utils import ColumnTable

from apollo.integrations.databricks.connector_patches import install_connector_patches


def _duplicate_name_table(ids: list, emails: list) -> pyarrow.Table:
    return pyarrow.Table.from_arrays(
        [pyarrow.array(ids, pyarrow.int64()), pyarrow.array(emails, pyarrow.string())],
        names=["id", "id"],
    )


class DatabricksConcatDuplicateColumnNamesTests(TestCase):
    # databricks-sql-connector 4.2.6–4.7.x failed these with "ArrowInvalid: Can't unify
    # schema with duplicate field names" (databricks-sql-python#989). They run against the
    # connector itself, so a release that regresses fails them.
    def test_single_chunk_with_duplicate_names(self):
        table = _duplicate_name_table([1, 2], ["a", "b"])

        result = utils.concat_table_chunks([table])

        self.assertEqual(["id", "id"], result.column_names)
        self.assertEqual([[1, 2], ["a", "b"]], [c.to_pylist() for c in result.columns])

    def test_multiple_chunks_with_duplicate_names(self):
        chunks = [
            _duplicate_name_table([1, 2], ["a", "b"]),
            _duplicate_name_table([3], ["c"]),
        ]

        result = utils.concat_table_chunks(chunks)

        self.assertEqual(3, result.num_rows)
        self.assertEqual(
            [[1, 2, 3], ["a", "b", "c"]], [c.to_pylist() for c in result.columns]
        )

    def test_empty_and_data_chunks_with_duplicate_names(self):
        chunks = [
            _duplicate_name_table([], []),
            _duplicate_name_table([1], ["a"]),
        ]

        result = utils.concat_table_chunks(chunks)

        self.assertEqual(1, result.num_rows)
        self.assertEqual([[1], ["a"]], [c.to_pylist() for c in result.columns])

    def test_fetchall_returns_rows_with_duplicate_names(self):
        # Drives the connector's own fetchall (Arrow concat + Row conversion) without a
        # server: a single buffered chunk and no more rows to fetch.
        table = _duplicate_name_table([1, 2], ["a", "b"])
        results = Mock(spec=["remaining_rows"])
        results.remaining_rows.return_value = table
        rs = result_set.ThriftResultSet.__new__(result_set.ThriftResultSet)
        rs.results = results
        rs.has_been_closed_server_side = False
        rs.has_more_rows = False
        rs._next_row_index = 0
        rs.description = [("id", "bigint"), ("id", "string")]
        rs.connection = Mock(disable_pandas=False)

        rows = rs.fetchall()

        self.assertEqual([(1, "a"), (2, "b")], [tuple(row) for row in rows])

    def test_unique_names_still_promote(self):
        # The nullability fix from databricks-sql-python#418: a null-typed column is
        # promoted to the other chunk's type.
        chunks = [
            pyarrow.Table.from_arrays([pyarrow.nulls(1)], names=["id"]),
            pyarrow.Table.from_arrays([pyarrow.array([1])], names=["id"]),
        ]

        result = utils.concat_table_chunks(chunks)

        self.assertEqual([None, 1], result.column("id").to_pylist())

    def test_column_table_path_unchanged(self):
        chunks = [
            ColumnTable([[1], ["a"]], ["id", "id"]),
            ColumnTable([[2], ["b"]], ["id", "id"]),
        ]

        result = utils.concat_table_chunks(chunks)

        self.assertIsInstance(result, ColumnTable)
        self.assertEqual([[1, 2], ["a", "b"]], result.column_table)


class DatabricksConnectorVersionTests(TestCase):
    def test_connector_includes_duplicate_column_names_fix(self):
        major, minor = (int(part) for part in sql.__version__.split(".")[:2])
        self.assertGreaterEqual((major, minor), (4, 8), sql.__version__)


class DatabricksRetiredConnectorPatchTests(TestCase):
    def test_install_connector_patches_is_a_no_op(self):
        # data-collector and hermes-agent still call this; see the connector_patches docstring.
        self.assertIsNone(install_connector_patches())
