from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class TransformStep:
    type: str
    input: Dict[str, Any]
    output: Dict[str, str]
    when: Optional[str] = None  # Jinja2 boolean expression over raw/derived/context (e.g. "raw.ssl_ca_pem is defined")
    field_map: Dict[str, Any] = field(default_factory=dict)  # contributed to client_args only if this step executes


@dataclass
class MapperConfig:
    name: str
    output_schema: str
    field_map: Dict[str, Any]
    passthrough: bool = False


@dataclass
class CcpConfig:
    name: str
    steps: List[TransformStep]
    mapper: MapperConfig


@dataclass
class PipelineState:
    raw: Dict[str, Any]
    derived: Dict[str, Any] = field(default_factory=dict)
    context: Dict[str, Any] = field(default_factory=dict)
