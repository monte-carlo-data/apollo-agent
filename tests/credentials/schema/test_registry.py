"""Tests that every documented self-hosted-credentials connection type is
backed by a registered schema, plus a couple of unit-level checks on the
lookup function.

If this test fails after adding a new connector, declare its schema on the
CtpConfig (or the proxy client class attribute, for non-CTP) — see
``apollo/credentials/schema/__init__.py`` for the lookup contract.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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


# ---------------------------------------------------------------------------
# Custom connector registry tests
#
# The custom connector loaders read from /opt/custom-connectors at import
# time and cache the result at module level.  We mock at the definition site
# so the tests never touch the filesystem or interact with the cache.
# ---------------------------------------------------------------------------

_CC_LOADER = "apollo.integrations.custom.custom_connector_loader"
_ETL_LOADER = "apollo.integrations.custom_etl.custom_etl_connector_loader"


@patch(
    f"{_CC_LOADER}.load_manifest",
    return_value={
        "connection_type": "custom-connector-abc1234",
        "credentials_schema": {"connect_args": {"type": "dict", "required": True}},
    },
)
@patch(
    f"{_CC_LOADER}.get_custom_connector_registry",
    return_value={
        "custom-connector-abc1234": "/fake/path",
    },
)
def test_custom_connector_with_credentials_schema(
    _mock_registry: MagicMock,
    _mock_manifest: MagicMock,
) -> None:
    schema = get_credentials_schema("custom-connector-abc1234")
    assert schema == {"connect_args": {"type": "dict", "required": True}}


@patch(
    f"{_CC_LOADER}.load_manifest",
    return_value={
        "connection_type": "custom-connector-abc1234",
    },
)
@patch(
    f"{_CC_LOADER}.get_custom_connector_registry",
    return_value={
        "custom-connector-abc1234": "/fake/path",
    },
)
def test_custom_connector_without_credentials_schema_returns_none(
    _mock_registry: MagicMock,
    _mock_manifest: MagicMock,
) -> None:
    assert get_credentials_schema("custom-connector-abc1234") is None


@patch(
    f"{_ETL_LOADER}.load_manifest",
    return_value={
        "connection_type": "custom-etl-connector-abc1234",
        "credentials_schema": {"connect_args": {"type": "dict", "required": True}},
    },
)
@patch(
    f"{_ETL_LOADER}.get_custom_etl_connector_registry",
    return_value={
        "custom-etl-connector-abc1234": "/fake/etl/path",
    },
)
def test_custom_etl_connector_with_credentials_schema(
    _mock_registry: MagicMock,
    _mock_manifest: MagicMock,
) -> None:
    schema = get_credentials_schema("custom-etl-connector-abc1234")
    assert schema == {"connect_args": {"type": "dict", "required": True}}


@patch(
    f"{_ETL_LOADER}.load_manifest",
    return_value={
        "connection_type": "custom-etl-connector-abc1234",
    },
)
@patch(
    f"{_ETL_LOADER}.get_custom_etl_connector_registry",
    return_value={
        "custom-etl-connector-abc1234": "/fake/etl/path",
    },
)
def test_custom_etl_connector_without_credentials_schema_returns_none(
    _mock_registry: MagicMock,
    _mock_manifest: MagicMock,
) -> None:
    assert get_credentials_schema("custom-etl-connector-abc1234") is None


@patch(f"{_ETL_LOADER}.get_custom_etl_connector_registry", return_value={})
@patch(f"{_CC_LOADER}.get_custom_connector_registry", return_value={})
def test_custom_connector_not_in_registry_returns_none(
    _mock_cc_registry: MagicMock,
    _mock_etl_registry: MagicMock,
) -> None:
    assert get_credentials_schema("custom-connector-unknown") is None


@patch(
    f"{_CC_LOADER}.load_manifest",
    return_value={
        "connection_type": "custom-connector-abc1234",
        "credentials_schema": ["not", "a", "dict"],
    },
)
@patch(
    f"{_CC_LOADER}.get_custom_connector_registry",
    return_value={
        "custom-connector-abc1234": "/fake/path",
    },
)
def test_custom_connector_non_dict_credentials_schema_returns_none(
    _mock_registry: MagicMock,
    _mock_manifest: MagicMock,
) -> None:
    """When credentials_schema is not a dict (e.g. a list), return None."""
    assert get_credentials_schema("custom-connector-abc1234") is None


@patch(
    f"{_CC_LOADER}.load_manifest",
    return_value={
        "connection_type": "custom-connector-abc1234",
        "credentials_schema": {"connect_args": {"type": "dict", "required": True}},
    },
)
@patch(
    f"{_CC_LOADER}.get_custom_connector_registry",
    return_value={
        "custom-connector-abc1234": "/fake/path",
    },
)
def test_custom_connector_schema_works_without_feature_gate(
    _mock_registry: MagicMock,
    _mock_manifest: MagicMock,
) -> None:
    # Credential validation is read-only and intentionally bypasses
    # MCD_CUSTOM_CONNECTORS_ENABLED — operators should be able to validate
    # credentials even when the connector is not yet enabled for collection.
    schema = get_credentials_schema("custom-connector-abc1234")
    assert schema == {"connect_args": {"type": "dict", "required": True}}
