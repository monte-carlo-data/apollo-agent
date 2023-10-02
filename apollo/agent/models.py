from dataclasses import dataclass, field
from typing import Optional, Any, List, Dict

from dataclasses_json import dataclass_json, config

from apollo.agent.utils import exclude_empty_values


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


@dataclass_json
@dataclass
class AgentOperation:
    trace_id: str
    commands: List[AgentCommand]

    @classmethod
    def from_dict(cls, param) -> "AgentOperation":
        pass

    def to_dict(self) -> Dict:
        pass


@dataclass_json
@dataclass
class AgentHealthInformation:
    platform: str
    version: str
    build: str
    env: Dict
    platform_info: Optional[Dict] = field(
        metadata=config(exclude=exclude_empty_values), default=None
    )
    trace_id: Optional[str] = field(
        metadata=config(exclude=exclude_empty_values), default=None
    )

    def to_dict(self) -> Dict:
        pass
