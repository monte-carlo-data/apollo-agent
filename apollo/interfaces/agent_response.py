from dataclasses import dataclass
from typing import Dict


@dataclass
class AgentResponse:
    """
    Object that represent a response to be sent to the client, includes the result object and the status code to send.
    """

    result: Dict
    status_code: int
