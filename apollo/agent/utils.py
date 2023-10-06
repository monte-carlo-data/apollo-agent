import sys
import traceback
from typing import Optional, Dict, List, Any

from apollo.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_EXCEPTION,
    ATTRIBUTE_NAME_STACK_TRACE,
)
from apollo.interfaces.agent_response import AgentResponse


# used so we don't include an empty platform info
def exclude_empty_values(value: Any) -> bool:
    return not bool(value)


# used so we don't include null values in json objects
def exclude_none_values(value: Any) -> bool:
    return value is None


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
            cls._response_for_error(message, stack_trace=stack_trace),
            status_code,
            trace_id,
        )

    @classmethod
    def response_for_last_exception(cls, prefix: Optional[str] = None) -> Dict:
        last_type, last_value, _ = sys.exc_info()
        error = str(last_value)
        exception_message = " ".join(
            traceback.format_exception_only(last_type, last_value)
        )
        if prefix:
            error = f"{prefix} {error}"
        stack_trace = traceback.format_tb(last_value.__traceback__)  # type: ignore
        return cls._response_for_error(
            error, exception_message=exception_message, stack_trace=stack_trace
        )

    @staticmethod
    def _response_for_error(
        message: str,
        exception_message: Optional[str] = None,
        stack_trace: Optional[List] = None,
    ) -> Dict:
        response: Dict[str, Any] = {
            ATTRIBUTE_NAME_ERROR: message,
        }
        if exception_message:
            response[ATTRIBUTE_NAME_EXCEPTION] = exception_message
        if stack_trace:
            response[ATTRIBUTE_NAME_STACK_TRACE] = stack_trace
        return response
