from dataclasses import dataclass
from io import BufferedReader
from typing import Dict, Optional, Any, BinaryIO

from apollo.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_TRACE_ID,
    ATTRIBUTE_NAME_RESULT,
)


@dataclass
class AgentResponse:
    """
    Object that represents a response to be sent to the client, includes the result object and the status code to send.
    """

    result: Any
    status_code: int
    trace_id: Optional[str] = None

    def __post_init__(self):
        if not self._is_binary_response(self.result) and not self._is_error_response(
            self.result
        ):
            self.result = {ATTRIBUTE_NAME_RESULT: self.result}
        if self.trace_id and isinstance(self.result, Dict):
            self.result[ATTRIBUTE_NAME_TRACE_ID] = self.trace_id

    @property
    def object_result(self):
        if isinstance(self.result, Dict):
            return self.result.get(ATTRIBUTE_NAME_RESULT)
        return None

    @object_result.setter
    def object_result(self, value: Any):
        self.result[ATTRIBUTE_NAME_RESULT] = value

    @staticmethod
    def _is_binary_response(result: Any):
        return (
            isinstance(result, bytes)
            or isinstance(result, BinaryIO)
            or isinstance(result, BufferedReader)
        )

    @staticmethod
    def _is_error_response(result: Any):
        return isinstance(result, Dict) and ATTRIBUTE_NAME_ERROR in result
