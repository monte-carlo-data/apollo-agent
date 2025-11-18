import re
from typing import Any, List, Dict

from apollo.agent.constants import ATTRIBUTE_VALUE_REDACTED, LOG_ATTRIBUTE_TRACE_ID

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
    re.compile(r".*password.*"),
    re.compile(r".*secret.*"),
    re.compile(r".*token.*"),
    re.compile(r".*key.*"),
    re.compile(r".*auth.*"),
    re.compile(r".*credential.*"),
]
_SKIP_REDACT_ATTRIBUTES = [LOG_ATTRIBUTE_TRACE_ID]


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
                    if cls._is_attribute_included(k, attributes)
                    else (
                        v
                        if cls._is_attribute_included(k, _SKIP_REDACT_ATTRIBUTES, True)
                        else cls.redact_attributes(v, attributes)
                    )
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
    def _is_attribute_included(
        key: str, attributes: List[str], exact_match: bool = False
    ) -> bool:
        return (
            key in attributes
            if exact_match
            else any(a in key.lower() for a in attributes)
        )

    @staticmethod
    def _redact_string(value: str) -> str:
        for expression in _REDACT_VALUE_EXPRESSIONS:
            if expression.match(value):
                return ATTRIBUTE_VALUE_REDACTED
        return value
