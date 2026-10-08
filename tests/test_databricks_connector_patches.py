import os
import re
import subprocess
import sys
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock

import pyarrow
from databricks import sql
from databricks.sql import result_set, utils
from databricks.sql.utils import ColumnTable

# Importing the proxy client installs the connector patches, as it does in production.
import apollo.integrations.databricks.databricks_sql_warehouse_proxy_client  # noqa: F401
from apollo.integrations.databricks import connector_patches
from apollo.integrations.databricks.connector_patches import install_connector_patches


def _duplicate_name_table(ids: list, emails: list) -> pyarrow.Table:
    return pyarrow.Table.from_arrays(
        [pyarrow.array(ids, pyarrow.int64()), pyarrow.array(emails, pyarrow.string())],
        names=["id", "id"],
    )


class DatabricksConcatDuplicateColumnNamesTests(TestCase):
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

    def test_result_set_call_path_is_patched(self):
        # result_set binds concat_table_chunks at import time; this is the name the
        # fetch*_arrow methods actually call.
        table = _duplicate_name_table([1], ["a"])

        result = result_set.concat_table_chunks([table, table])

        self.assertEqual(2, result.num_rows)

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

    def test_different_types_still_raise(self):
        chunks = [
            _duplicate_name_table([1], ["a"]),
            pyarrow.Table.from_arrays(
                [pyarrow.array([2]), pyarrow.array([3])], names=["id", "id"]
            ),
        ]

        with self.assertRaisesRegex(pyarrow.ArrowInvalid, "Can't unify schema"):
            utils.concat_table_chunks(chunks)

    def test_different_names_still_raise(self):
        chunks = [
            _duplicate_name_table([1], ["a"]),
            pyarrow.Table.from_arrays(
                [pyarrow.array([2]), pyarrow.array(["b"])], names=["id", "email"]
            ),
        ]

        with self.assertRaisesRegex(pyarrow.ArrowInvalid, "Can't unify schema"):
            utils.concat_table_chunks(chunks)

    def test_unique_names_still_promote(self):
        # The connector's own behaviour: a null-typed column is promoted to the other
        # chunk's type. The patch must not get in the way of that.
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


class DatabricksConnectorPatchInstallTests(TestCase):
    def test_install_is_idempotent(self):
        patched = getattr(utils, "_concat_arrow_tables")

        install_connector_patches()

        self.assertIs(patched, getattr(utils, "_concat_arrow_tables"))

    def test_install_skips_when_helper_is_missing(self):
        patched = getattr(utils, "_concat_arrow_tables")
        delattr(utils, "_concat_arrow_tables")
        try:
            with self.assertLogs(connector_patches.logger, "WARNING"):
                install_connector_patches()
            self.assertFalse(hasattr(utils, "_concat_arrow_tables"))
        finally:
            setattr(utils, "_concat_arrow_tables", patched)

    def test_empty_chunk_list_reraises_original_error(self):
        concat = Mock(side_effect=pyarrow.ArrowInvalid("boom"))

        with self.assertRaisesRegex(pyarrow.ArrowInvalid, "boom"):
            connector_patches._tolerate_duplicate_column_names(concat)([])

    def test_unique_names_reraise_original_error(self):
        # Matching schemas alone don't prove duplicate names caused the failure; an error
        # on a schema with unique names must surface instead of falling back.
        concat = Mock(side_effect=pyarrow.ArrowInvalid("boom"))
        table = pyarrow.Table.from_arrays(
            [pyarrow.array([1]), pyarrow.array(["a"])], names=["id", "email"]
        )

        with self.assertRaisesRegex(pyarrow.ArrowInvalid, "boom"):
            connector_patches._tolerate_duplicate_column_names(concat)([table, table])

    def test_extra_arguments_are_forwarded(self):
        table = _duplicate_name_table([1], ["a"])
        concat = Mock(return_value=table)

        result = connector_patches._tolerate_duplicate_column_names(concat)(
            [table], "positional", keyword="value"
        )

        self.assertIs(table, result)
        concat.assert_called_once_with([table], "positional", keyword="value")


class DatabricksConnectorPatchWiringTests(TestCase):
    def test_proxy_client_import_installs_patch(self):
        # A fresh interpreter, so this test process having installed the patch already
        # can't mask a missing call.
        code = (
            "import apollo.integrations.databricks.databricks_sql_warehouse_proxy_client\n"
            "from databricks.sql import utils\n"
            "patched = getattr(utils._concat_arrow_tables, "
            "'_apollo_tolerates_duplicate_column_names', False)\n"
            "raise SystemExit(0 if patched else 1)\n"
        )
        env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
        completed = subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)

    def test_every_databricks_sql_importer_installs_patch(self):
        # The test above only covers the module we know about; this scan enforces the
        # convention for any other module that imports databricks.sql.
        apollo_root = Path(__file__).resolve().parents[1] / "apollo"
        import_pattern = re.compile(
            r"^\s*(from databricks import (?:[\w ,]*,\s*)?sql\b"
            r"|from databricks\.sql\b|import databricks\.sql\b)",
            re.MULTILINE,
        )
        # Column 0 only: indented calls inside functions and comments don't count.
        call_pattern = re.compile(r"^install_connector_patches\(\)", re.MULTILINE)
        patch_module = (
            apollo_root / "integrations" / "databricks" / "connector_patches.py"
        )
        texts = {
            path: path.read_text()
            for path in apollo_root.rglob("*.py")
            if path != patch_module
        }
        importers = [
            path for path, text in texts.items() if import_pattern.search(text)
        ]

        self.assertTrue(
            importers, "no databricks.sql importers found; check the scan path/regex"
        )
        offenders = sorted(
            str(path.relative_to(apollo_root))
            for path in importers
            if not call_pattern.search(texts[path])
        )
        self.assertEqual(
            [],
            offenders,
            "modules import databricks.sql without calling install_connector_patches() "
            f"at module level: {offenders}",
        )


class DatabricksConnectorVersionTests(TestCase):
    def test_connector_version_is_one_the_patch_was_validated_against(self):
        # When this fails after a connector bump, run
        # test_unpatched_connector_still_rejects_duplicate_names:
        # 1. It passes: the bug is still there, so re-validate the patch and widen this range.
        # 2. Its assertion fails: upstream fixed it, so make install_connector_patches() a
        #    no-op. Keep the function and module: other packages import it.
        # 3. The helper is missing: DatabricksConcatDuplicateColumnNamesTests decides
        #    between 1 and 2. See the databricks CLAUDE.md.
        major, minor = (int(part) for part in sql.__version__.split(".")[:2])
        self.assertIn((major, minor), {(4, 5), (4, 6)}, sql.__version__)

    def test_unpatched_connector_still_rejects_duplicate_names(self):
        # The wrapper uses functools.wraps, so __wrapped__ is the connector's own helper.
        helper = getattr(utils, "_concat_arrow_tables", None)
        if helper is None:
            self.fail(
                "databricks.sql.utils._concat_arrow_tables is gone, so "
                "install_connector_patches() no longer patches anything. If "
                "DatabricksConcatDuplicateColumnNamesTests fail, the bug is still there: "
                "re-target the patch at the connector's new merge helper. If they pass, "
                "upstream fixed it: retire the patch."
            )
        original = helper.__wrapped__
        with self.assertRaisesRegex(
            pyarrow.ArrowInvalid,
            "duplicate field names",
            msg="the connector no longer rejects duplicate column names; make "
            "install_connector_patches() a no-op (keep the module, others import it)",
        ):
            original([_duplicate_name_table([1], ["a"])])
