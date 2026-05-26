"""Lookup for a connection type's self-hosted credentials schema.

Two paths:

1. **CTP-enrolled connectors** (the vast majority): the schema lives on
   the registered :class:`apollo.integrations.ctp.models.CtpConfig` as the
   ``raw_credentials_schema`` field — a cerberus schema dict declared
   alongside the existing ``MapperConfig`` so a developer modifying the
   raw inputs naturally sees the schema in the same file.

2. **Non-CTP connectors** (currently only ``clickhouse`` and
   ``salesforce-data-cloud``): the schema lives on the proxy client class
   as a ``SELF_HOSTED_CREDENTIALS_SCHEMA`` class attribute. Lazy imports
   avoid pulling heavyweight drivers into this module's import path.
"""

from __future__ import annotations

from typing import Any, Callable

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


def get_credentials_schema(connection_type: str) -> dict[str, Any] | None:
    """Return the cerberus schema dict for ``connection_type``, or ``None``.

    ``None`` means "no schema declared for this connection type" — either it
    isn't supported for self-hosted credentials, or a schema simply hasn't
    been added yet. The caller (typically the validate endpoint) should
    treat ``None`` as a 400 with a clear "not supported" message.
    """
    from apollo.integrations.ctp.registry import CtpRegistry

    ctp_config = CtpRegistry.get(connection_type)
    if ctp_config is not None and ctp_config.raw_credentials_schema is not None:
        return ctp_config.raw_credentials_schema

    resolver = _NON_CTP_PROXY_RESOLVERS.get(connection_type)
    if resolver is not None:
        proxy_class = resolver()
        schema = getattr(proxy_class, "SELF_HOSTED_CREDENTIALS_SCHEMA", None)
        if isinstance(schema, dict):
            return schema

    return None
