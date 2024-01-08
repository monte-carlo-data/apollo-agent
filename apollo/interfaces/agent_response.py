import json
from dataclasses import dataclass
from io import BufferedReader
from typing import Dict, Optional, Any, BinaryIO

from apollo.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT_LOCATION,
    ATTRIBUTE_NAME_TRACE_ID,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_RESULT_COMPRESSED,
)
from apollo.agent.serde import AgentSerializer


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

    def use_location(self, location: str):
        self.result[ATTRIBUTE_NAME_RESULT_LOCATION] = location
        if ATTRIBUTE_NAME_RESULT in self.result:
            self.result.pop(ATTRIBUTE_NAME_RESULT)

    @property
    def compressed(self) -> bool:
        return self.result.get(ATTRIBUTE_NAME_RESULT_COMPRESSED, False)

    @compressed.setter
    def compressed(self, compressed: bool):
        self.result[ATTRIBUTE_NAME_RESULT_COMPRESSED] = compressed

    @property
    def is_error(self) -> bool:
        return self._is_error_response(self.result)

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

    def calculate_result_size(self) -> int:
        if not self.result or self._is_binary_response(self.result):
            return 0
        return len(self.serialize_result().encode())

    def serialize_result(self, unwrap_result: bool = False) -> str:
        if unwrap_result and ATTRIBUTE_NAME_RESULT in self.result:
            result = self.result[ATTRIBUTE_NAME_RESULT]
        else:
            result = self.result

        return json.dumps(result, cls=AgentSerializer)
