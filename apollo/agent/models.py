from dataclasses import dataclass, field
from typing import Optional, Any, List, Dict, Tuple, Union

from dataclasses_json import DataClassJsonMixin, config


# used so we don't include an empty platform info
from apollo.agent.constants import RESPONSE_TYPE_JSON, RESPONSE_TYPE_URL
from apollo.agent.serde import rows_encoder


def exclude_empty_values(value: Any) -> bool:
    return not bool(value)


# used so we don't include null values in json objects
def exclude_none_values(value: Any) -> bool:
    return value is None


class AgentError(Exception):
    pass


class AgentConfigurationError(AgentError):
    pass


class AgentRequestError(AgentError):
    pass


@dataclass
class AgentCommand(DataClassJsonMixin):
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


@dataclass(kw_only=True)
class AgentOperation(DataClassJsonMixin):
    trace_id: str
    response_size_limit_bytes: int = 0
    compress_response_threshold_bytes: int = (
        0  # configures the threshold to send compressed responses inline, disabled by default
    )
    response_type: str = RESPONSE_TYPE_JSON
    skip_cache: bool = False
    compress_response_file: bool = (
        False  # indicates if response files should be compressed
    )

    def __post_init__(self):
        if self.response_type not in (RESPONSE_TYPE_URL, RESPONSE_TYPE_JSON):
            raise AgentRequestError(
                f"Invalid response_type '{self.response_type}'. Must be one of {RESPONSE_TYPE_URL}, {RESPONSE_TYPE_JSON}"
            )

    def can_use_pre_signed_url(self) -> bool:
        return (
            0 < self.response_size_limit_bytes
            or self.response_type == RESPONSE_TYPE_URL
        )

    def must_use_pre_signed_url(self, size: int) -> bool:
        return (
            0 < self.response_size_limit_bytes < size
        ) or self.response_type == RESPONSE_TYPE_URL

    def must_compress_response_file(self) -> bool:
        # RESPONSE_TYPE_URL is used to send results to the UI, compression is not supported
        return self.response_type == RESPONSE_TYPE_JSON and self.compress_response_file

    def must_unwrap_result(self) -> bool:
        return self.response_type == RESPONSE_TYPE_URL

    def can_compress_response(self) -> bool:
        return (
            0 < self.compress_response_threshold_bytes
            and self.response_type == RESPONSE_TYPE_JSON
        )

    def must_compress_response(self, size: int) -> bool:
        return (
            0 < self.compress_response_threshold_bytes < size
        ) and self.response_type == RESPONSE_TYPE_JSON


@dataclass(kw_only=True)
class AgentCommands(AgentOperation):
    commands: List[AgentCommand]


@dataclass(kw_only=True)
class AgentScriptModule:
    source: str
    name: str


@dataclass(kw_only=True)
class AgentScript(AgentOperation):
    entry_module: str
    modules: List[AgentScriptModule]
    kwargs: Dict


@dataclass
class AgentHealthInformation(DataClassJsonMixin):
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
    warnings: Optional[List[str]] = field(
        metadata=config(exclude=exclude_none_values), default=None
    )


@dataclass
class AgentExecuteSqlQueryResponse(DataClassJsonMixin):
    """Response schema for the built-in execute_sql_query command."""

    columns: List[str]
    rows: Union[List[List[Any]], List[Tuple], List[Dict]]
    is_partial: bool = False

    def __post_init__(self):
        self.rows = rows_encoder(self.rows)
