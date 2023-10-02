from dataclasses import dataclass, field
from typing import Optional, Any, List, Dict

from dataclasses_json import dataclass_json, config

ATTRIBUTE_NAME_ERROR = "__error__"
ATTRIBUTE_NAME_ERROR_TYPE = "__error_type__"
ATTRIBUTE_NAME_EXCEPTION = "__exception__"
ATTRIBUTE_NAME_STACK_TRACE = "__stack_trace__"
ATTRIBUTE_NAME_REFERENCE = "__reference__"
ATTRIBUTE_NAME_TYPE = "__type__"

ATTRIBUTE_VALUE_TYPE_CALL = "call"

CONTEXT_VAR_CLIENT = "_client"
CONTEXT_VAR_UTILS = "__utils"


def _exclude_none_values(value: Any) -> bool:
    return value is None


class AgentError(Exception):
    pass


class AgentWrappedError(Exception):
    def __init__(self, message: str, error_type: str):
        super().__init__(message)
        self.error_type = error_type


@dataclass_json
@dataclass
class AgentCommand:
    method: str

    # configure fields to be excluded when value is None, to reduce size of log messages
    target: Optional[str] = field(
        metadata=config(exclude=_exclude_none_values), default=None
    )
    args: Optional[List[Any]] = field(
        metadata=config(exclude=_exclude_none_values), default=None
    )
    kwargs: Optional[Dict] = field(
        metadata=config(exclude=_exclude_none_values), default=None
    )
    store: Optional[str] = field(
        metadata=config(exclude=_exclude_none_values), default=None
    )
    next: Optional["AgentCommand"] = field(
        metadata=config(exclude=_exclude_none_values), default=None
    )

    @staticmethod
    def from_dict(param) -> "AgentCommand":
        pass


@dataclass_json
@dataclass
class AgentOperation:
    trace_id: str
    commands: List[AgentCommand]
    skip_cache: bool = False

    @staticmethod
    def from_dict(param) -> "AgentOperation":
        pass

    def to_dict(self) -> Dict:
        pass
