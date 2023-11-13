from typing import Dict, Optional

from apollo.agent.constants import LOG_ATTRIBUTE_TRACE_ID, LOG_ATTRIBUTE_OPERATION_NAME


class LoggingUtils:
    def __init__(self):
        def builder(trace_id: Optional[str], operation_name: str, extra: Dict):
            extra = {
                LOG_ATTRIBUTE_OPERATION_NAME: operation_name,
                **extra,
            }
            if trace_id:
                extra[LOG_ATTRIBUTE_TRACE_ID] = trace_id
            return extra

        self.extra_builder = builder

    def build_extra(
        self,
        trace_id: Optional[str],
        operation_name: str,
        extra: Optional[Dict] = None,
    ) -> Dict:
        return self.extra_builder(trace_id, operation_name, extra or {})
