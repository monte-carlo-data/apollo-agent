import sys
import traceback
from typing import Optional, Dict, List


class AgentUtils:
    @classmethod
    def response_for_last_exception(cls, prefix: Optional[str] = None) -> Dict:
        last_type, last_value, _ = sys.exc_info()
        error = " ".join(traceback.format_exception_only(last_type, last_value))
        if prefix:
            error = f"{prefix} {error}"
        stack_trace = traceback.format_tb(last_value.__traceback__)
        return cls.response_for_error(error, stack_trace)

    @staticmethod
    def response_for_error(message: str, stack_trace: Optional[List] = None) -> Dict:
        response = {
            "__error__": message,
        }
        if stack_trace:
            response["__stack_trace__"] = stack_trace
        return response
