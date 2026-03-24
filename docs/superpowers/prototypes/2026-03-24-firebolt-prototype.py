"""
Firebolt Integration — Feasibility Prototype
=============================================
Date: 2026-03-24
Driver: firebolt-sdk (PyPI: firebolt-sdk >= 1.18.5)
Auth: Service Account / OAuth 2.0 Client Credentials
  POST https://id.app.firebolt.io/oauth/token
  grant_type=client_credentials, client_id, client_secret
  Token valid for 86,400 seconds.

Analog: SnowflakeProxyClient
  (apollo/integrations/snowflake/snowflake_proxy_client.py)

Credential shape expected under connect_args:
  {
    "connect_args": {
      "client_id":     "<service-account-client-id>",
      "client_secret": "<service-account-client-secret>",
      "account":       "<firebolt-account-name>",        # e.g. "my-org"
      "database":      "<database-name>",
      "engine":        "<engine-name>",                  # optional; omit for system engine
    }
  }

Known limitations discovered during research:
  - Query history retention capped at 10,000 queries per engine cluster.
  - information_schema.tables.last_altered is always NULL; only created is meaningful.
  - Metadata queries require an active, running engine — system engine works for
    information_schema but engine_query_history requires a user engine.
  - No native lineage views in information_schema.
  - AWS PrivateLink is in public preview; Azure PrivateLink is not documented.
  - OAuth tokens expire after 86,400 seconds; the SDK handles refresh automatically
    when using ClientCredentials, but long-lived agent processes should be aware.
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List, Optional, Tuple

# firebolt-sdk >= 1.18.5
# Install: pip install firebolt-sdk
from firebolt.client.auth import ClientCredentials
from firebolt.db import connect  # PEP-249 DB-API 2.0 connection factory

_ATTR_CONNECT_ARGS = "connect_args"

# ---------------------------------------------------------------------------
# SQL templates (sourced directly from web research example_query fields)
# ---------------------------------------------------------------------------

_SQL_TABLES = """\
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type,
    number_of_rows,
    compressed_bytes,
    uncompressed_bytes,
    created
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema')
"""

_SQL_COLUMNS = """\
SELECT
    table_catalog,
    table_schema,
    table_name,
    column_name,
    ordinal_position,
    data_type,
    is_nullable,
    column_default,
    is_in_primary_index
FROM information_schema.columns
WHERE table_schema NOT IN ('information_schema')
ORDER BY table_schema, table_name, ordinal_position
"""

# NOTE: engine_query_history is only available when connected to a user engine
# (not the system engine). Retention is capped at the 10,000 most recent queries
# per engine cluster — this is a hard platform limit.
_SQL_QUERY_LOGS = """\
SELECT
    account_name,
    user_name,
    service_account_name,
    submitted_time,
    start_time,
    end_time,
    duration_us,
    status,
    query_id,
    query_text,
    scanned_rows,
    returned_rows,
    error_message
FROM information_schema.engine_query_history
WHERE submitted_time > NOW() - INTERVAL '24 HOURS'
ORDER BY submitted_time DESC
LIMIT 1000
"""

# Volume: compressed/uncompressed bytes and row counts per table.
_SQL_VOLUME = """\
SELECT
    table_catalog,
    table_schema,
    table_name,
    number_of_rows,
    compressed_bytes,
    uncompressed_bytes
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema')
ORDER BY uncompressed_bytes DESC
"""

# Freshness: only creation time is natively available; last_altered is always NULL.
_SQL_FRESHNESS = """\
SELECT
    table_catalog,
    table_schema,
    table_name,
    created
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema')
"""


# ---------------------------------------------------------------------------
# FireboltProxyClient
# ---------------------------------------------------------------------------


class FireboltProxyClient:
    """
    Proxy client for Firebolt using the firebolt-sdk (PEP-249) driver.

    Credentials must be supplied under the "connect_args" key and will be
    passed to firebolt.db.connect.  The SDK handles OAuth 2.0 token exchange
    automatically when a ClientCredentials auth object is provided.

    Follows the same structural pattern as SnowflakeProxyClient:
      apollo/integrations/snowflake/snowflake_proxy_client.py

    Suggested credential shape:
      {
        "connect_args": {
          "client_id":     "<service-account-client-id>",
          "client_secret": "<service-account-client-secret>",
          "account":       "<firebolt-account-name>",
          "database":      "<database-name>",
          "engine":        "<engine-name>",   # optional
        }
      }
    """

    def __init__(self, credentials: Optional[Dict[str, Any]], **kwargs: Any) -> None:
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Firebolt agent client requires '{_ATTR_CONNECT_ARGS}' in credentials"
            )

        args: Dict[str, Any] = dict(credentials[_ATTR_CONNECT_ARGS])

        # Extract service-account credentials and build a ClientCredentials auth object.
        # The SDK exchanges these for a Bearer token at connection time and refreshes
        # automatically before expiry (tokens are valid for 86,400 s).
        client_id: str = args.pop("client_id")
        client_secret: str = args.pop("client_secret")

        auth = ClientCredentials(
            client_id=client_id,
            client_secret=client_secret,
            # use_token_cache=True is the default; avoids redundant token requests
            # across connections within the same process lifetime.
            use_token_cache=True,
        )

        # firebolt.db.connect accepts:
        #   auth        — ClientCredentials (or UsernamePassword for interactive use)
        #   account     — Firebolt account name (required)
        #   database    — target database (optional; defaults to account default)
        #   engine      — engine name (optional; omit to use system engine)
        #   api_endpoint — override for VPC/PrivateLink endpoints
        self._connection = connect(auth=auth, **args)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    @property
    def wrapped_client(self):
        """Return the underlying DB-API connection (mirrors BaseDbProxyClient pattern)."""
        return self._connection

    def close(self) -> None:
        """Close the underlying connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __del__(self) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _execute(self, sql: str, max_rows: int = 100) -> Tuple[List[str], List[tuple]]:
        """Execute *sql* and return (column_names, rows)."""
        with self._connection.cursor() as cursor:
            cursor.execute(sql)
            description: list = cursor.description or []
            columns = [col[0] for col in description]
            rows = cursor.fetchmany(max_rows)
        return columns, rows

    # ------------------------------------------------------------------
    # Connection test
    # ------------------------------------------------------------------

    def test_connection(self) -> bool:
        """
        Verify that the connection is live by running a trivial query.
        Returns True on success; raises on failure.
        """
        columns, rows = self._execute("SELECT 1 AS probe")
        assert rows and rows[0][0] == 1, "Unexpected result from probe query"
        return True

    # ------------------------------------------------------------------
    # Metadata queries
    # ------------------------------------------------------------------

    def get_tables(self) -> Tuple[List[str], List[tuple]]:
        """
        Return all user tables from information_schema.tables.

        Columns: table_catalog, table_schema, table_name, table_type,
                 number_of_rows, compressed_bytes, uncompressed_bytes, created

        Note: last_altered is always NULL in Firebolt; use `created` only.
        """
        return self._execute(_SQL_TABLES)

    def get_columns(self, table: Optional[str] = None) -> Tuple[List[str], List[tuple]]:
        """
        Return column metadata from information_schema.columns.

        Columns: table_catalog, table_schema, table_name, column_name,
                 ordinal_position, data_type, is_nullable, column_default,
                 is_in_primary_index

        Args:
            table: Optional table name filter (schema.table or bare table name).
                   When None, returns columns for all user tables.
        """
        sql = _SQL_COLUMNS
        if table:
            # Append a simple filter; production code should use parameterised queries.
            parts = table.split(".", 1)
            if len(parts) == 2:
                schema, tbl = parts
                sql += f" AND table_schema = '{schema}'" f" AND table_name = '{tbl}'"
            else:
                sql += f" AND table_name = '{table}'"
        return self._execute(sql)

    def get_query_logs(self) -> Tuple[List[str], List[tuple]]:
        """
        Return the last 24 hours of query history from
        information_schema.engine_query_history.

        IMPORTANT: This view requires an active user engine connection — it is
        NOT available on the system engine. The platform retains only the 10,000
        most recent queries per engine cluster (hard limit, not configurable).

        Columns: account_name, user_name, service_account_name, submitted_time,
                 start_time, end_time, duration_us, status, query_id, query_text,
                 scanned_rows, returned_rows, error_message
        """
        return self._execute(_SQL_QUERY_LOGS)

    def get_volume(self, table: Optional[str] = None) -> Tuple[List[str], List[tuple]]:
        """
        Return storage volume metrics (row counts + byte sizes) for user tables.

        Sourced from information_schema.tables.
        Columns: table_catalog, table_schema, table_name, number_of_rows,
                 compressed_bytes, uncompressed_bytes

        Args:
            table: Optional table name filter (schema.table or bare table name).
        """
        sql = _SQL_VOLUME
        if table:
            parts = table.split(".", 1)
            if len(parts) == 2:
                schema, tbl = parts
                sql += f" AND table_schema = '{schema}'" f" AND table_name = '{tbl}'"
            else:
                sql += f" AND table_name = '{table}'"
        return self._execute(sql)

    def get_freshness(
        self, table: Optional[str] = None
    ) -> Tuple[List[str], List[tuple]]:
        """
        Return table freshness information.

        LIMITATION: information_schema.tables.last_altered is always NULL in
        Firebolt.  Only the `created` timestamp is natively available.  A proxy
        for freshness can be derived from engine_query_history (last DML against
        the table), but that approach is bounded by the 10,000-query retention
        cap and requires additional query text parsing.

        Columns: table_catalog, table_schema, table_name, created

        Args:
            table: Optional table name filter (schema.table or bare table name).
        """
        sql = _SQL_FRESHNESS
        if table:
            parts = table.split(".", 1)
            if len(parts) == 2:
                schema, tbl = parts
                sql += f" AND table_schema = '{schema}'" f" AND table_name = '{tbl}'"
            else:
                sql += f" AND table_name = '{table}'"
        return self._execute(sql)


# ---------------------------------------------------------------------------
# __main__ block — runs all methods and prints results as JSON
# ---------------------------------------------------------------------------


def _rows_to_dicts(columns: List[str], rows: List[tuple]) -> List[Dict[str, Any]]:
    """Convert (columns, rows) to a list of dicts for JSON-friendly output."""
    return [dict(zip(columns, row)) for row in rows]


def main(credentials: Dict[str, Any]) -> None:
    """
    Instantiate FireboltProxyClient, run every method, and print results.

    Args:
        credentials: Dict containing "connect_args" with Firebolt connection
                     parameters (client_id, client_secret, account, database,
                     and optionally engine).
    """
    print("=== Firebolt Feasibility Prototype ===\n")

    client = FireboltProxyClient(credentials=credentials)

    results: Dict[str, Any] = {}

    # 1. Connection test
    print("[1] test_connection ...")
    ok = client.test_connection()
    results["connection_test"] = ok
    print(f"    -> {ok}\n")

    # 2. Tables
    print("[2] get_tables ...")
    cols, rows = client.get_tables()
    sample = _rows_to_dicts(cols, rows[:3])
    results["tables"] = {"total_rows": len(rows), "sample": sample}
    print(
        f"    -> {len(rows)} rows returned; sample:\n{json.dumps(sample, indent=4, default=str)}\n"
    )

    # 3. Columns (no filter — all tables)
    print("[3] get_columns ...")
    cols, rows = client.get_columns()
    sample = _rows_to_dicts(cols, rows[:3])
    results["columns"] = {"total_rows": len(rows), "sample": sample}
    print(
        f"    -> {len(rows)} rows returned; sample:\n{json.dumps(sample, indent=4, default=str)}\n"
    )

    # 4. Query logs
    print("[4] get_query_logs ...")
    print("    NOTE: requires an active user engine, not the system engine.")
    try:
        cols, rows = client.get_query_logs()
        sample = _rows_to_dicts(cols, rows[:3])
        results["query_logs"] = {"total_rows": len(rows), "sample": sample}
        print(
            f"    -> {len(rows)} rows returned; sample:\n{json.dumps(sample, indent=4, default=str)}\n"
        )
    except Exception as exc:
        results["query_logs"] = {"error": str(exc)}
        print(f"    -> ERROR: {exc}\n")

    # 5. Volume
    print("[5] get_volume ...")
    cols, rows = client.get_volume()
    sample = _rows_to_dicts(cols, rows[:3])
    results["volume"] = {"total_rows": len(rows), "sample": sample}
    print(
        f"    -> {len(rows)} rows returned; sample:\n{json.dumps(sample, indent=4, default=str)}\n"
    )

    # 6. Freshness
    print("[6] get_freshness ...")
    print("    NOTE: last_altered always NULL; only 'created' is available.")
    cols, rows = client.get_freshness()
    sample = _rows_to_dicts(cols, rows[:3])
    results["freshness"] = {"total_rows": len(rows), "sample": sample}
    print(
        f"    -> {len(rows)} rows returned; sample:\n{json.dumps(sample, indent=4, default=str)}\n"
    )

    print("=== Summary ===")
    print(json.dumps(results, indent=4, default=str))

    client.close()


if __name__ == "__main__":
    # ---------------------------------------------------------------------------
    # Supply credentials here (never commit real secrets).
    # ---------------------------------------------------------------------------
    _credentials: Dict[str, Any] = {
        "connect_args": {
            "client_id": "YOUR_SERVICE_ACCOUNT_CLIENT_ID",
            "client_secret": "YOUR_SERVICE_ACCOUNT_CLIENT_SECRET",
            "account": "YOUR_FIREBOLT_ACCOUNT_NAME",
            "database": "YOUR_DATABASE_NAME",
            # "engine":      "YOUR_ENGINE_NAME",  # omit to use the system engine
        }
    }

    if _credentials["connect_args"]["client_id"] == "YOUR_SERVICE_ACCOUNT_CLIENT_ID":
        print(
            "ERROR: Populate _credentials in the __main__ block before running.\n"
            "       Set client_id, client_secret, account, and database.",
            file=sys.stderr,
        )
        sys.exit(1)

    main(_credentials)
