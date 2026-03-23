# apollo/integrations/ccp/registry.py
from typing import Any

from apollo.integrations.ccp.errors import CcpPipelineError
from apollo.integrations.ccp.models import CcpConfig
from apollo.integrations.ccp.pipeline import CcpPipeline

_ATTR_CONNECT_ARGS = "connect_args"
_initialized: bool = False


def _discover() -> None:
    """Import all CCP default modules to trigger registration.

    Called once on first registry access. Add new connector imports here.
    """
    # ── Relational ────────────────────────────────────────────────────
    import apollo.integrations.ccp.defaults.postgres  # noqa: F401
    import apollo.integrations.ccp.defaults.redshift  # noqa: F401
    import apollo.integrations.ccp.defaults.mysql  # noqa: F401
    import apollo.integrations.ccp.defaults.sap_hana  # noqa: F401
    import apollo.integrations.ccp.defaults.salesforce_crm  # noqa: F401
    import apollo.integrations.ccp.defaults.dremio  # noqa: F401
    import apollo.integrations.ccp.defaults.oracle  # noqa: F401
    import apollo.integrations.ccp.defaults.snowflake  # noqa: F401
    import apollo.integrations.ccp.defaults.teradata  # noqa: F401
    import apollo.integrations.ccp.defaults.db2  # noqa: F401
    import apollo.integrations.ccp.defaults.bigquery  # noqa: F401

    # ── Distributed query engines ─────────────────────────────────────
    import apollo.integrations.ccp.defaults.hive  # noqa: F401
    import apollo.integrations.ccp.defaults.starburst_galaxy  # noqa: F401
    import apollo.integrations.ccp.defaults.starburst_enterprise  # noqa: F401

    # ── Databricks ────────────────────────────────────────────────────
    import apollo.integrations.ccp.defaults.databricks  # noqa: F401


def _ensure_initialized() -> None:
    global _initialized
    if not _initialized:
        _initialized = True
        _discover()


class CcpRegistry:
    _registry: dict[str, CcpConfig] = {}

    @classmethod
    def register(cls, connection_type: str, config: CcpConfig) -> None:
        cls._registry[connection_type] = config

    @classmethod
    def get(cls, connection_type: str) -> CcpConfig | None:
        _ensure_initialized()
        return cls._registry.get(connection_type)

    @classmethod
    def resolve(
        cls,
        connection_type: str,
        credentials: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Run the registered CCP pipeline for connection_type and return
        {"connect_args": <pipeline output>}.
        If credentials already have connect_args, returns unchanged (legacy path).
        Raises CcpPipelineError if connection_type is not registered.
        """
        _ensure_initialized()
        if _ATTR_CONNECT_ARGS in credentials:
            return credentials

        config = cls.get(connection_type)
        if config is None:
            raise CcpPipelineError(
                stage="registry",
                message=f"No CCP config registered for '{connection_type}'. Call CcpRegistry.get() before resolve().",
            )
        return {
            _ATTR_CONNECT_ARGS: CcpPipeline().execute(
                config, credentials, context=context or {}
            )
        }
