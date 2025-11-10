from typing import Any, List, Dict

from apollo.agent.constants import ATTRIBUTE_VALUE_REDACTED


class AgentRedactUtilities:
    @classmethod
    def redact_attributes(cls, value: Any, attributes: List[str]) -> Any:
        if isinstance(value, Dict):
            return {
                k: (
                    ATTRIBUTE_VALUE_REDACTED
                    if cls._is_redacted_attribute(k, attributes)
                    else cls.redact_attributes(v, attributes)
                )
                for k, v in value.items()
            }
        elif isinstance(value, List):
            return [cls.redact_attributes(v, attributes) for v in value]
        else:
            return value

    @staticmethod
    def _is_redacted_attribute(key: str, attributes: List[str]) -> bool:
        return any(a in key.lower() for a in attributes)
