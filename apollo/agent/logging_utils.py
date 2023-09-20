from typing import Dict, Any, List

_SENSITIVE_KEYWORDS = [
    "credentials",
    "password",
    "pwd",
    "token",
]


class LoggingUtils:
    def __init__(self):
        def builder(trace_id: str, operation_name: str, extra: Dict):
            return {
                "trace_id": trace_id,
                "operation_name": operation_name,
                **extra,
            }

        self.extra_builder = builder

    def build_extra(
        self,
        trace_id: str,
        operation_name: str,
        extra: Dict,
    ) -> Dict:
        redacted_extra = self._redact(
            self.extra_builder(trace_id, operation_name, extra)
        )
        return redacted_extra

    @classmethod
    def _redact(cls, values: Dict) -> Dict:
        return {key: cls._redact_value(key, value) for key, value in values.items()}

    @classmethod
    def _redact_value(cls, key: str, value: Any) -> Any:
        if cls._is_sensitive_key(key):
            return "[REDACTED]"
        elif isinstance(value, Dict):
            return cls._redact(value)
        elif isinstance(value, List):
            return [cls._redact(v) for v in value]
        else:
            return value

    @staticmethod
    def _is_sensitive_key(key: str) -> bool:
        key_lower = key.lower()
        return any(keyword in key_lower for keyword in _SENSITIVE_KEYWORDS)
