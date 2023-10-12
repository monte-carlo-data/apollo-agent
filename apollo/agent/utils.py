import os
import sys
import tempfile
import traceback
import uuid
from typing import Optional, Dict, List, BinaryIO, Any

from apollo.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_EXCEPTION,
    ATTRIBUTE_NAME_STACK_TRACE,
    ATTRIBUTE_NAME_ERROR_TYPE,
    ATTRIBUTE_VALUE_REDACTED,
)
from apollo.integrations.base_proxy_client import BaseProxyClient
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
        status_code: int = 200,
        trace_id: Optional[str] = None,
        client: Optional[BaseProxyClient] = None,
    ):
        return AgentResponse(
            cls.response_for_last_exception(prefix=prefix, client=client),
            status_code,
            trace_id,
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
    def response_for_last_exception(
        cls, client: Optional[BaseProxyClient] = None, prefix: Optional[str] = None
    ) -> Dict:
        last_type, last_value, _ = sys.exc_info()
        error = str(last_value)
        exception_message = " ".join(
            traceback.format_exception_only(last_type, last_value)
        )
        if prefix:
            error = f"{prefix} {error}"
        stack_trace = traceback.format_tb(last_value.__traceback__)
        return cls._response_for_error(
            error,
            exception_message=exception_message,
            stack_trace=stack_trace,
            error_type=cls._get_error_type(last_value, client),
        )

    @staticmethod
    def temp_file_path() -> str:
        return os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))

    @staticmethod
    def open_file(path: str) -> BinaryIO:
        return open(path, "rb")

    @classmethod
    def redact_attributes(cls, value: Any, attributes: List[str]) -> Any:
        if isinstance(value, Dict):
            return {
                k: ATTRIBUTE_VALUE_REDACTED
                if k in attributes
                else cls.redact_attributes(v, attributes)
                for k, v in value.items()
            }
        elif isinstance(value, List):
            return [cls.redact_attributes(v, attributes) for v in value]
        else:
            return value

    @staticmethod
    def _get_error_type(
        error: Exception, client: Optional[BaseProxyClient] = None
    ) -> Optional[str]:
        if client:
            return client.get_error_type(error)
        return None

    @staticmethod
    def _response_for_error(
        message: str,
        exception_message: Optional[str] = None,
        stack_trace: Optional[List] = None,
        error_type: Optional[str] = None,
    ) -> Dict:
        response = {
            ATTRIBUTE_NAME_ERROR: message,
        }
        if exception_message:
            response[ATTRIBUTE_NAME_EXCEPTION] = exception_message
        if stack_trace:
            response[ATTRIBUTE_NAME_STACK_TRACE] = stack_trace
        if error_type:
            response[ATTRIBUTE_NAME_ERROR_TYPE] = error_type
        return response
