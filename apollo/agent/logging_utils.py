from typing import Dict


class LoggingUtils:
    def __init__(self):
        def builder(trace_id: str, extra: Dict):
            return {
                "trace_id": trace_id,
                **extra,
            }

        self.extra_builder = builder

    def build_extra(
        self,
        trace_id: str,
        extra: Dict,
    ) -> Dict:
        return self.extra_builder(trace_id, extra)
