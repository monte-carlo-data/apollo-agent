import sys
import traceback
from typing import Optional, Dict, List

from apollo.interfaces.agent_response import AgentResponse


class AgentUtils:
    @staticmethod
    def agent_ok_response(result: Dict):
        return AgentResponse(result, 200)

    @classmethod
    def agent_response_for_last_exception(
        cls, prefix: Optional[str] = None, status_code: int = 500
    ):
        return AgentResponse(cls.response_for_last_exception(prefix), status_code)

    @classmethod
    def agent_response_for_error(
        cls, message: str, stack_trace: Optional[List] = None, status_code: int = 200
    ):
        return AgentResponse(cls.response_for_error(message, stack_trace), status_code)

    @classmethod
    def response_for_last_exception(cls, prefix: Optional[str] = None) -> Dict:
        last_type, last_value, _ = sys.exc_info()
        error = str(last_value)
        if prefix:
            error = f"{prefix} {error}"
        stack_trace = traceback.format_stack()
        return cls.response_for_error(error, stack_trace)

    @staticmethod
    def response_for_error(message: str, stack_trace: Optional[List] = None) -> Dict:
        response = {
            "__error__": message,
        }
        if stack_trace:
            response["__stack_trace__"] = stack_trace
        return response
