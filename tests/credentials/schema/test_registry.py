"""Tests that every documented self-hosted-credentials connection type is
backed by a registered schema, plus a couple of unit-level checks on the
lookup function.

If this test fails after adding a new connector, declare its schema on the
CtpConfig (or the proxy client class attribute, for non-CTP) — see
``apollo/credentials/schema/__init__.py`` for the lookup contract.
"""

from __future__ import annotations

import pytest

from apollo.credentials.schema import get_credentials_schema


# Source of truth: the connection types listed under "Which integrations
# support self-hosted credentials?" in
# docs-website/docs/Architecture/arch-resources/self-hosted-credentials/index.md.
# Looker-GIT shares the `git` connection key with the regular Git connector.
DOCUMENTED_CONNECTION_TYPES = [
    "azure-dedicated-sql-pool",
    "azure-sql-database",
    "bigquery",
    "clickhouse",
    "databricks",
    "db2",
    "dremio",
    "fivetran",
    "git",  # Looker-GIT shares this key.
    "informatica",
    "informatica-v2",
    "looker",
    "microsoft-fabric",
    "motherduck",
    "mysql",
    "oracle",
    "postgres",
    "power-bi",
    "redshift",
    "salesforce-crm",
    "salesforce-data-cloud",
    "sap-hana",
    "snowflake",
    "sql-server",
    "starburst-enterprise",
    "starburst-galaxy",
    "tableau",
    "teradata",
]


@pytest.mark.parametrize("connection_type", DOCUMENTED_CONNECTION_TYPES)
def test_every_documented_connector_has_a_schema(connection_type: str) -> None:
    schema = get_credentials_schema(connection_type)
    assert isinstance(schema, dict) and schema, (
        f"No cerberus schema declared for connection type {connection_type!r}; "
        f"either add raw_credentials_schema to its CtpConfig or "
        f"declare SELF_HOSTED_CREDENTIALS_SCHEMA on its proxy client."
    )


def test_unknown_connection_type_returns_none() -> None:
    assert get_credentials_schema("totally-bogus-connection-type") is None


def test_infrastructure_connectors_return_none() -> None:
    # Infrastructure connectors are CTP-enrolled (for credential resolution)
    # but not exposed via self-hosted credentials. They must NOT return a
    # schema or the validate endpoint would mislead callers into thinking
    # the connector is supported for self-hosting.
    for ct in ("http", "mulesoft", "hive", "presto", "athena", "glue", "s3"):
        assert get_credentials_schema(ct) is None, (
            f"{ct} should not have a self-hosted-credentials schema "
            "(it is an infrastructure connector, not in the public docs)."
        )
