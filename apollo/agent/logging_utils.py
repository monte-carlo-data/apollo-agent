from typing import Dict


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
        return self.extra_builder(trace_id, operation_name, extra)
