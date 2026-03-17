# apollo/integrations/ccp/registry.py
from typing import Any

from apollo.integrations.ccp.models import CcpConfig

_ATTR_CONNECT_ARGS = "connect_args"


class CcpRegistry:
    _registry: dict[str, CcpConfig] = {}

    @classmethod
    def register(cls, connection_type: str, config: CcpConfig) -> None:
        cls._registry[connection_type] = config

    @classmethod
    def get(cls, connection_type: str) -> CcpConfig | None:
        return cls._registry.get(connection_type)

    @classmethod
    def resolve(cls, connection_type: str, credentials: dict[str, Any]) -> dict[str, Any]:
        """
        If a CCP config is registered for this connection type and credentials are in the
        flat shape (no connect_args key), run the CCP pipeline and return
        {"connect_args": <pipeline output>}. Otherwise return credentials unchanged.
        """
        if _ATTR_CONNECT_ARGS not in credentials:
            config = cls.get(connection_type)
            if config:
                from apollo.integrations.ccp.pipeline import CcpPipeline
                return {_ATTR_CONNECT_ARGS: CcpPipeline().execute(config, credentials)}
        return credentials
