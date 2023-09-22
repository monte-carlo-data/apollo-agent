import sys
import traceback
from typing import Optional, Dict, List

from apollo.agent.models import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_EXCEPTION,
    ATTRIBUTE_NAME_STACK_TRACE,
)
from apollo.interfaces.agent_response import AgentResponse


class AgentUtils:
    """
    Helper methods to create success/failure responses.
    """

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
        return AgentResponse(
            cls.response_for_error(message, stack_trace=stack_trace), status_code
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
        stack_trace = traceback.format_tb(last_value.__traceback__)
        return cls.response_for_error(
            error, exception_message=exception_message, stack_trace=stack_trace
        )

    @staticmethod
    def response_for_error(
        message: str,
        exception_message: Optional[str] = None,
        stack_trace: Optional[List] = None,
    ) -> Dict:
        response = {
            ATTRIBUTE_NAME_ERROR: message,
        }
        if exception_message:
            response[ATTRIBUTE_NAME_EXCEPTION] = exception_message
        if stack_trace:
            response[ATTRIBUTE_NAME_STACK_TRACE] = stack_trace
        return response
