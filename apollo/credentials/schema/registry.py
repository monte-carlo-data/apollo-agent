"""Lookup for a connection type's self-hosted credentials schema.

Three paths:

1. **CTP-enrolled connectors** (the vast majority): the schema lives on
   the registered :class:`apollo.integrations.ctp.models.CtpConfig` as the
   ``raw_credentials_schema`` field — a cerberus schema dict declared
   alongside the existing ``MapperConfig`` so a developer modifying the
   raw inputs naturally sees the schema in the same file.

2. **Non-CTP connectors** (currently only ``clickhouse`` and
   ``salesforce-data-cloud``): the schema lives on the proxy client class
   as a ``SELF_HOSTED_CREDENTIALS_SCHEMA`` class attribute. Lazy imports
   avoid pulling heavyweight drivers into this module's import path.

3. **Custom connectors** (``custom-connector-*`` and
   ``custom-etl-connector-*``): the schema is declared as an optional
   ``credentials_schema`` key in the connector's ``manifest.json``, read
   from disk at ``/opt/custom-connectors/<name>/`` or
   ``/opt/custom-etl-connectors/<name>/``. The lookup reuses the existing
   connector loader registries and ``load_manifest()`` helpers.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)

_NonCtpResolver = Callable[[], type]


def _resolve_clickhouse() -> type:
    from apollo.integrations.db.clickhouse_proxy_client import ClickHouseProxyClient

    return ClickHouseProxyClient


def _resolve_salesforce_data_cloud() -> type:
    from apollo.integrations.db.salesforce_data_cloud_proxy_client import (
        SalesforceDataCloudProxyClient,
    )

    return SalesforceDataCloudProxyClient


_NON_CTP_PROXY_RESOLVERS: dict[str, _NonCtpResolver] = {
    "clickhouse": _resolve_clickhouse,
    "salesforce-data-cloud": _resolve_salesforce_data_cloud,
}


def _resolve_custom_schema(
    connection_type: str,
    get_registry_fn: Callable[[], dict[str, str]],
    load_manifest_fn: Callable[[str], dict[str, Any]],
    label: str,
) -> dict[str, Any] | None:
    """Shared helper: look up ``credentials_schema`` from a custom connector manifest.

    Parameters
    ----------
    connection_type:
        The connection type to resolve.
    get_registry_fn:
        Callable that returns ``{connection_type: connector_dir, ...}``.
    load_manifest_fn:
        Callable that reads and returns the manifest dict from a connector dir.
    label:
        Human-readable label for log messages (e.g. "custom connector").
    """
    try:
        registry = get_registry_fn()
        connector_dir = registry.get(connection_type)
    except Exception:
        logger.debug(
            "Failed to load %s registry for %s",
            label,
            connection_type,
            exc_info=True,
        )
        return None

    if connector_dir is None:
        return None

    # Connector is registered — failures here are real errors, not "not found".
    manifest = load_manifest_fn(connector_dir)
    schema = manifest.get("credentials_schema")
    if isinstance(schema, dict):
        return schema
    if schema is not None:
        logger.warning(
            "credentials_schema for %s is not a dict (got %s); ignoring",
            connection_type,
            type(schema).__name__,
        )
    return None


def _resolve_custom_connector(connection_type: str) -> dict[str, Any] | None:
    """Return ``credentials_schema`` from a custom warehouse connector's manifest."""
    from apollo.integrations.custom.custom_connector_loader import (
        get_custom_connector_registry,
        load_manifest,
    )

    return _resolve_custom_schema(
        connection_type,
        get_registry_fn=get_custom_connector_registry,
        load_manifest_fn=load_manifest,
        label="custom connector",
    )


def _resolve_custom_etl_connector(connection_type: str) -> dict[str, Any] | None:
    """Return ``credentials_schema`` from a custom ETL connector's manifest."""
    from apollo.integrations.custom_etl.custom_etl_connector_loader import (
        get_custom_etl_connector_registry,
        load_manifest,
    )

    return _resolve_custom_schema(
        connection_type,
        get_registry_fn=get_custom_etl_connector_registry,
        load_manifest_fn=load_manifest,
        label="custom ETL connector",
    )


def get_credentials_schema(connection_type: str) -> dict[str, Any] | None:
    """Return the cerberus schema dict for ``connection_type``, or ``None``.

    ``None`` means "no schema declared for this connection type" — either it
    isn't supported for self-hosted credentials, or a schema simply hasn't
    been added yet. The caller (typically the validate endpoint) should
    treat ``None`` as a 400 with a clear "not supported" message.
    """
    from apollo.integrations.ctp.registry import CtpRegistry

    # Path 1: CTP-enrolled connectors
    ctp_config = CtpRegistry.get(connection_type)
    if ctp_config is not None and ctp_config.raw_credentials_schema is not None:
        return ctp_config.raw_credentials_schema

    # Path 2: Non-CTP proxy clients with class-level schemas
    resolver = _NON_CTP_PROXY_RESOLVERS.get(connection_type)
    if resolver is not None:
        proxy_class = resolver()
        schema = getattr(proxy_class, "SELF_HOSTED_CREDENTIALS_SCHEMA", None)
        if isinstance(schema, dict):
            return schema

    # Path 3: Custom connectors (manifest-declared schemas)
    schema = _resolve_custom_connector(connection_type)
    if schema is not None:
        return schema

    schema = _resolve_custom_etl_connector(connection_type)
    if schema is not None:
        return schema

    return None
