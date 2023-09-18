from dataclasses import dataclass
from typing import Optional, Any, List, Dict

from dataclasses_json import dataclass_json

ATTRIBUTE_NAME_ERROR = "__error__"
ATTRIBUTE_NAME_EXCEPTION = "__exception__"
ATTRIBUTE_NAME_STACK_TRACE = "__stack_trace__"
ATTRIBUTE_NAME_REFERENCE = "__reference__"
ATTRIBUTE_NAME_TYPE = "__type__"

ATTRIBUTE_VALUE_TYPE_CALL = "call"

CONTEXT_VAR_CLIENT = "_client"
CONTEXT_VAR_UTILS = "__utils"


class AgentError(Exception):
    pass


@dataclass_json
@dataclass
class AgentCommand:
    method: str
    target: Optional[str] = None
    args: Optional[List[Any]] = None
    kwargs: Optional[Dict] = None
    store: Optional[str] = None
    next: Optional["AgentCommand"] = None

    @staticmethod
    def from_dict(param) -> "AgentCommand":
        pass


@dataclass_json
@dataclass
class AgentOperation:
    trace_id: str
    commands: List[AgentCommand]

    @staticmethod
    def from_dict(param) -> "AgentOperation":
        pass

    def to_dict(self) -> Dict:
        pass
