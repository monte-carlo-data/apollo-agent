from dataclasses import dataclass, field
from typing import Any


@dataclass
class TransformStep:
    type: str
    input: dict[str, Any]
    output: dict[str, str]
    when: str | None = None  # Jinja2 boolean expression over raw/derived/context (e.g. "raw.ssl_ca_pem is defined")
    field_map: dict[str, Any] = field(default_factory=dict)  # contributed to client_args only if this step executes


@dataclass
class MapperConfig:
    name: str
    output_schema: str
    field_map: dict[str, Any]
    passthrough: bool = False


@dataclass
class CcpConfig:
    name: str
    steps: list[TransformStep]
    mapper: MapperConfig


@dataclass
class PipelineState:
    raw: dict[str, Any]
    derived: dict[str, Any] = field(default_factory=dict)
    context: dict[str, Any] = field(default_factory=dict)
