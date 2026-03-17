from typing import Any

from apollo.integrations.ccp.errors import CcpPipelineError
from apollo.integrations.ccp.transforms.base import Transform


class TransformRegistry:
    _registry: dict[str, type[Transform]] = {}

    @classmethod
    def register(cls, type_name: str, transform_class: type[Transform]) -> None:
        cls._registry[type_name] = transform_class

    @classmethod
    def get(cls, type_name: str) -> Transform:
        if type_name not in cls._registry:
            raise CcpPipelineError(
                stage="transform_lookup",
                message=f"Unknown transform type: '{type_name}'. Registered types: {sorted(cls._registry.keys())}",
            )
        return cls._registry[type_name]()


# Self-registration: import all known primitives so they register themselves
from apollo.integrations.ccp.transforms import tmp_file_write as _  # noqa: F401, E402
from apollo.integrations.ccp.transforms import decode_bytes as _  # noqa: F401, E402
