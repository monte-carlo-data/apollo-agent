from dataclasses import dataclass
from typing import Dict, Optional


_TRACE_ID_ATTR = "__trace_id__"


@dataclass
class AgentResponse:
    """
    Object that represents a response to be sent to the client, includes the result object and the status code to send.
    """

    result: Dict
    status_code: int
    trace_id: Optional[str] = None

    def __post_init__(self):
        if self.trace_id:
            self.result[_TRACE_ID_ATTR] = self.trace_id
