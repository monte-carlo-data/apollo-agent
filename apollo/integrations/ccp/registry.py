# apollo/integrations/ccp/registry.py
from typing import Any

from apollo.integrations.ccp.defaults.passthrough import PASSTHROUGH_CCP
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
        cls, connection_type: str, credentials: dict[str, Any]
    ) -> dict[str, Any]:
        """
        If credentials are in the flat shape (no connect_args key), run the CCP pipeline
        and return {"connect_args": <pipeline output>}. Uses the registered config for
        connection_type, or passthrough if none is registered.
        If credentials already have connect_args, returns unchanged (legacy path).
        """
        _ensure_initialized()
        if _ATTR_CONNECT_ARGS in credentials:
            return credentials

        config = cls.get(connection_type) or PASSTHROUGH_CCP
        return {_ATTR_CONNECT_ARGS: CcpPipeline().execute(config, credentials)}
