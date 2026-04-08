# apollo/integrations/ctp/registry.py
from typing import Any

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import CtpConfig
from apollo.integrations.ctp.pipeline import CtpPipeline

_ATTR_CONNECT_ARGS = "connect_args"
_initialized: bool = False


def _discover() -> None:
    """Import all CTP default modules to trigger registration.

    Called once on first registry access. Add new connector imports here as
    their proxy clients are updated in Phase 2 to read from connect_args.
    """
    import apollo.integrations.ctp.defaults.aws  # noqa: F401
    import apollo.integrations.ctp.defaults.bigquery  # noqa: F401
    import apollo.integrations.ctp.defaults.databricks  # noqa: F401
    import apollo.integrations.ctp.defaults.db2  # noqa: F401
    import apollo.integrations.ctp.defaults.dremio  # noqa: F401
    import apollo.integrations.ctp.defaults.fabric  # noqa: F401
    import apollo.integrations.ctp.defaults.git  # noqa: F401
    import apollo.integrations.ctp.defaults.http  # noqa: F401
    import apollo.integrations.ctp.defaults.hive  # noqa: F401
    import apollo.integrations.ctp.defaults.motherduck  # noqa: F401
    import apollo.integrations.ctp.defaults.presto  # noqa: F401
    import apollo.integrations.ctp.defaults.snowflake  # noqa: F401
    import apollo.integrations.ctp.defaults.teradata  # noqa: F401
    import apollo.integrations.ctp.defaults.starburst_galaxy  # noqa: F401
    import apollo.integrations.ctp.defaults.redshift  # noqa: F401
    import apollo.integrations.ctp.defaults.sap_hana  # noqa: F401
    import apollo.integrations.ctp.defaults.salesforce_crm  # noqa: F401
    import apollo.integrations.ctp.defaults.starburst_enterprise  # noqa: F401
    import apollo.integrations.ctp.defaults.postgres  # noqa: F401
    import apollo.integrations.ctp.defaults.sql_server  # noqa: F401
    import apollo.integrations.ctp.defaults.tableau  # noqa: F401
    import apollo.integrations.ctp.defaults.power_bi  # noqa: F401
    import apollo.integrations.ctp.defaults.looker  # noqa: F401
    import apollo.integrations.ctp.defaults.mysql  # noqa: F401
    import apollo.integrations.ctp.defaults.oracle  # noqa: F401


def _ensure_initialized() -> None:
    global _initialized
    if not _initialized:
        _initialized = True
        _discover()


class CtpRegistry:
    _registry: dict[str, CtpConfig] = {}

    @classmethod
    def register(cls, connection_type: str, config: CtpConfig) -> None:
        cls._registry[connection_type] = config

    @classmethod
    def get(cls, connection_type: str) -> CtpConfig | None:
        _ensure_initialized()
        return cls._registry.get(connection_type)

    @classmethod
    def resolve_custom(
        cls,
        connection_type: str,
        credentials: dict[str, Any],
        ctp_config: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Resolve credentials using a caller-supplied CTP config dict.

        The TypedDict schema from the registered default for connection_type is
        injected into the custom config's mapper so the output contract is preserved.
        Follows the same connect_args unwrap-and-run path as resolve().
        """
        _ensure_initialized()
        config = CtpConfig.from_dict(ctp_config)
        registered = cls._registry.get(connection_type)
        if registered is not None:
            config.mapper.schema = registered.mapper.schema
        raw_or_connect_args = credentials.get(_ATTR_CONNECT_ARGS, credentials)
        if not isinstance(raw_or_connect_args, dict):
            return credentials
        return {
            _ATTR_CONNECT_ARGS: CtpPipeline().execute(
                config, raw_or_connect_args, context=context or {}
            )
        }

    @classmethod
    def resolve(
        cls,
        connection_type: str,
        credentials: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Run the registered CTP pipeline for connection_type and return
        {"connect_args": <pipeline output>}.
        If credentials contain connect_args (DC pre-shaped path), the inner dict
        is unwrapped and run through the pipeline — both flat and pre-shaped
        credentials follow the same transform path.
        Raises CtpPipelineError if connection_type is not registered.
        """
        _ensure_initialized()
        config = cls.get(connection_type)
        if config is None:
            raise CtpPipelineError(
                stage="registry",
                message=f"No CTP config registered for '{connection_type}'. Call CtpRegistry.get() before resolve().",
            )
        # Unwrap pre-shaped connect_args so both flat and DC-pre-shaped credentials
        # follow the same transform path through the pipeline.
        # If connect_args is not a dict (e.g. a pre-built ODBC string), pass through
        # unchanged — the pipeline cannot interpret non-dict credentials.
        raw_or_connect_args = credentials.get(_ATTR_CONNECT_ARGS, credentials)
        if not isinstance(raw_or_connect_args, dict):
            return credentials
        return {
            _ATTR_CONNECT_ARGS: CtpPipeline().execute(
                config, raw_or_connect_args, context=context or {}
            )
        }
