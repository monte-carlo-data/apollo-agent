from apollo.integrations.ccp.errors import CcpPipelineError
from apollo.integrations.ccp.transforms.base import Transform

_initialized: bool = False


def _discover() -> None:
    """Import all transform modules to trigger registration.

    Called once on first registry access. Add new transform imports here.
    """
    import apollo.integrations.ccp.transforms.tmp_file_write  # noqa: F401
    import apollo.integrations.ccp.transforms.resolve_ssl_options  # noqa: F401


def _ensure_initialized() -> None:
    global _initialized
    if not _initialized:
        _initialized = True
        _discover()


class TransformRegistry:
    _registry: dict[str, type[Transform]] = {}

    @classmethod
    def register(cls, type_name: str, transform_class: type[Transform]) -> None:
        cls._registry[type_name] = transform_class

    @classmethod
    def get(cls, type_name: str) -> Transform:
        _ensure_initialized()
        if type_name not in cls._registry:
            raise CcpPipelineError(
                stage="transform_lookup",
                message=f"Unknown transform type: '{type_name}'. Registered types: {sorted(cls._registry.keys())}",
            )
        return cls._registry[type_name]()
