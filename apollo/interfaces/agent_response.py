from dataclasses import dataclass
from typing import Dict


@dataclass
class AgentResponse:
    result: Dict
    status_code: int
