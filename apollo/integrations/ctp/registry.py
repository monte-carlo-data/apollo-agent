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
    import apollo.integrations.ctp.defaults.fabric  # noqa: F401
    import apollo.integrations.ctp.defaults.starburst_galaxy  # noqa: F401
    import apollo.integrations.ctp.defaults.redshift  # noqa: F401
    import apollo.integrations.ctp.defaults.sap_hana  # noqa: F401
    import apollo.integrations.ctp.defaults.salesforce_crm  # noqa: F401


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
        raw = credentials.get(_ATTR_CONNECT_ARGS, credentials)
        return {
            _ATTR_CONNECT_ARGS: CtpPipeline().execute(
                config, raw, context=context or {}
            )
        }
