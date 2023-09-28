import sys
import traceback
from typing import Optional, Dict, List

from apollo.interfaces.agent_response import AgentResponse


class AgentUtils:
    """
    Helper methods to create success/failure responses.
    """

    @staticmethod
    def agent_ok_response(result: Dict, trace_id: Optional[str] = None):
        return AgentResponse(result, 200, trace_id)

    @classmethod
    def agent_response_for_last_exception(
        cls,
        prefix: Optional[str] = None,
        status_code: int = 500,
        trace_id: Optional[str] = None,
    ):
        return AgentResponse(
            cls.response_for_last_exception(prefix), status_code, trace_id
        )

    @classmethod
    def agent_response_for_error(
        cls,
        message: str,
        stack_trace: Optional[List] = None,
        status_code: int = 200,
        trace_id: Optional[str] = None,
    ):
        return AgentResponse(
            cls._response_for_error(message, stack_trace), status_code, trace_id
        )

    @classmethod
    def response_for_last_exception(cls, prefix: Optional[str] = None) -> Dict:
        last_type, last_value, _ = sys.exc_info()
        error = str(last_value)
        if prefix:
            error = f"{prefix} {error}"
        stack_trace = traceback.format_tb(last_value.__traceback__)
        return cls._response_for_error(error, stack_trace)

    @staticmethod
    def _response_for_error(message: str, stack_trace: Optional[List] = None) -> Dict:
        response = {
            "__error__": message,
        }
        if stack_trace:
            response["__stack_trace__"] = stack_trace
        return response
