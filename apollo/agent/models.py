from dataclasses import dataclass
from typing import Optional, Any, List, Dict

from dataclasses_json import dataclass_json


class AgentError(Exception):
    pass


@dataclass
class AgentOperationResponse:
    result: Dict
    status_code: int


@dataclass_json
@dataclass
class AgentCommand:
    method: str
    target: Optional[str] = None
    args: Optional[List[Any]] = None
    kwargs: Optional[Dict] = None
    store: Optional[str] = None
    next: Optional["AgentCommand"] = None


@dataclass_json
@dataclass
class AgentOperation:
    operation_name: str
    trace_id: str
    commands: List[AgentCommand]

    @classmethod
    def from_dict(cls, param) -> "AgentOperation":
        pass

    def to_dict(self) -> Dict:
        pass
