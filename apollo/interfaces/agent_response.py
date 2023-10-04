from dataclasses import dataclass
from io import BufferedReader
from typing import Dict, Optional, Any, BinaryIO

from apollo.agent.constants import ATTRIBUTE_NAME_ERROR

_TRACE_ID_ATTR = "__mcd_trace_id__"
_RESULT_ATTR = "__mcd_result__"


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
            self.result = {_RESULT_ATTR: self.result}
        if self.trace_id and isinstance(self.result, Dict):
            self.result[_TRACE_ID_ATTR] = self.trace_id

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
