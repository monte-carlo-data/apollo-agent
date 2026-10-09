"""
Runtime patches for databricks-sql-connector behaviour we can't fix by pinning.

Since 4.2.6 the connector concatenates Arrow result chunks with
``concat_tables(promote_options="default")``, which rejects duplicate column names
(``ArrowInvalid: Can't unify schema with duplicate field names``) even for a single chunk.
4.2.5's plain concat accepted them but caps thrift below its CVE fixes, so we fall back to
it when every chunk shares a schema that contains a duplicate name. Every module that imports
``databricks.sql`` must call ``install_connector_patches()`` at module level. Other packages
import ``install_connector_patches()`` from here, so its name and path must stay: retiring the
patch means turning it into a no-op. When to retire it: see the version checks in
``tests/test_databricks_connector_patches.py``.
"""

import functools
import logging
from typing import Any, Callable, List

import pyarrow
from databricks.sql import utils

logger = logging.getLogger(__name__)

_PATCHED_ATTR = "_apollo_tolerates_duplicate_column_names"


def install_connector_patches() -> None:
    _patch_concat_arrow_tables()


def _patch_concat_arrow_tables() -> None:
    # Patch the private helper rather than concat_table_chunks: result_set imports
    # concat_table_chunks by name, so replacing it in utils wouldn't reach the fetch
    # methods, while concat_table_chunks resolves _concat_arrow_tables at call time.
    original = getattr(utils, "_concat_arrow_tables", None)
    if original is None:
        logger.warning(
            "databricks.sql.utils._concat_arrow_tables not found, skipping the duplicate "
            "column names patch; check whether this connector version still needs it"
        )
        return
    if getattr(original, _PATCHED_ATTR, False):
        return

    setattr(utils, "_concat_arrow_tables", _tolerate_duplicate_column_names(original))


def _tolerate_duplicate_column_names(
    concat: Callable[..., pyarrow.Table],
) -> Callable[..., pyarrow.Table]:
    # Forward any extra arguments: if upstream adds a parameter, a wrapper that only took
    # table_chunks would fail every Databricks query with a TypeError.
    @functools.wraps(concat)
    def concat_arrow_tables(
        table_chunks: List[pyarrow.Table], *args: Any, **kwargs: Any
    ) -> pyarrow.Table:
        try:
            return concat(table_chunks, *args, **kwargs)
        except pyarrow.ArrowInvalid:
            if not table_chunks:
                raise
            # Only the duplicate-name failure falls back: identical schemas with a repeated
            # name are always rejected by promotion, which is otherwise a no-op for them, so
            # the plain concat gives the same result. Anything else keeps the original error.
            first_schema = table_chunks[0].schema
            if len(set(first_schema.names)) == len(first_schema.names):
                raise
            if not all(chunk.schema.equals(first_schema) for chunk in table_chunks):
                raise
            return pyarrow.concat_tables(table_chunks)

    setattr(concat_arrow_tables, _PATCHED_ATTR, True)
    return concat_arrow_tables
