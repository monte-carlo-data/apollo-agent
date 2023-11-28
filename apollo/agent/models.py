from dataclasses import dataclass, field
from typing import Optional, Any, List, Dict

from dataclasses_json import dataclass_json, config


# used so we don't include an empty platform info
def exclude_empty_values(value: Any) -> bool:
    return not bool(value)


# used so we don't include null values in json objects
def exclude_none_values(value: Any) -> bool:
    return value is None


class AgentError(Exception):
    pass


class AgentConfigurationError(AgentError):
    pass


class AgentUpdateError(AgentError):
    pass


@dataclass_json
@dataclass
class AgentCommand:
    method: str

    # configure fields to be excluded when value is None, to reduce size of log messages
    target: Optional[str] = field(
        metadata=config(exclude=exclude_none_values), default=None
    )
    args: Optional[List[Any]] = field(
        metadata=config(exclude=exclude_none_values), default=None
    )
    kwargs: Optional[Dict] = field(
        metadata=config(exclude=exclude_none_values), default=None
    )
    store: Optional[str] = field(
        metadata=config(exclude=exclude_none_values), default=None
    )
    next: Optional["AgentCommand"] = field(
        metadata=config(exclude=exclude_none_values), default=None
    )

    @staticmethod
    def from_dict(param: Dict) -> "AgentCommand":  # type: ignore
        pass


@dataclass_json
@dataclass
class AgentOperation:
    trace_id: str
    commands: List[AgentCommand]
    response_size_limit_bytes: int = 0
    skip_cache: bool = False

    def can_use_pre_signed_url(self) -> bool:
        return 0 < self.response_size_limit_bytes

    def should_use_pre_signed_url(self, size: int) -> bool:
        return self.can_use_pre_signed_url() and self.response_size_limit_bytes < size

    @staticmethod
    def from_dict(param) -> "AgentOperation":  # type: ignore
        pass

    def to_dict(self) -> Dict:  # type: ignore
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
    extra: Optional[Dict] = field(
        metadata=config(exclude=exclude_none_values), default=None
    )

    def to_dict(self) -> Dict:  # type: ignore
        pass
