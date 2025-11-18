import re
from typing import Any, List, Dict

from apollo.agent.constants import ATTRIBUTE_VALUE_REDACTED

_STANDARD_REDACTED_ATTRIBUTES = [
    "pass",
    "secret",
    "client",
    "token",
    "user",
    "auth",
    "credential",
    "key",
]
_REDACT_VALUE_EXPRESSIONS = [
    re.compile(r"[a-zA-Z0-9_\-]{20,64}"),
    re.compile(r'.*password.*"'),
    re.compile(r'.*secret.*"'),
    re.compile(r'.*token.*"'),
    re.compile(r'.*key.*"'),
    re.compile(r'.*auth.*"'),
    re.compile(r'.*credential.*"'),
]


class AgentRedactUtilities:
    @classmethod
    def standard_redact(cls, value: Any):
        return cls.redact_attributes(value, _STANDARD_REDACTED_ATTRIBUTES)

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
        elif isinstance(value, str):
            return cls._redact_string(value)
        else:
            return value

    @staticmethod
    def _is_redacted_attribute(key: str, attributes: List[str]) -> bool:
        return any(a in key.lower() for a in attributes)

    @staticmethod
    def _redact_string(value: str) -> str:
        for expression in _REDACT_VALUE_EXPRESSIONS:
            if expression.match(value):
                return ATTRIBUTE_VALUE_REDACTED
        return value
