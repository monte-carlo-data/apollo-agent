from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class TransformStep:
    type: str
    input: dict[str, Any]
    output: dict[str, str]
    when: str | None = (
        None  # Jinja2 boolean expression over raw/derived/context (e.g. "raw.ssl_ca_pem is defined")
    )
    field_map: dict[str, Any] = field(
        default_factory=dict
    )  # contributed to client_args only if this step executes

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TransformStep:
        missing = {"type", "input", "output"} - data.keys()
        if missing:
            raise ValueError(
                f"TransformStep missing required fields: {sorted(missing)}"
            )
        return cls(
            type=data["type"],
            input=data["input"],
            output=data["output"],
            when=data.get("when"),
            field_map=data.get("field_map", {}),
        )


@dataclass
class MapperConfig:
    name: str
    field_map: dict[str, Any]
    schema: type | None = None
    passthrough: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MapperConfig:
        missing = {"name", "field_map"} - data.keys()
        if missing:
            raise ValueError(f"MapperConfig missing required fields: {sorted(missing)}")
        return cls(
            name=data["name"],
            field_map=data["field_map"],
            # schema is always None when deserializing from JSON —
            # it is injected at runtime from the registered CTP for the connection type
            schema=None,
            passthrough=data.get("passthrough", False),
        )


@dataclass
class CtpConfig:
    name: str
    steps: list[TransformStep]
    mapper: MapperConfig

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CtpConfig:
        missing = {"name", "steps", "mapper"} - data.keys()
        if missing:
            raise ValueError(f"CtpConfig missing required fields: {sorted(missing)}")
        return cls(
            name=data["name"],
            steps=[TransformStep.from_dict(s) for s in data["steps"]],
            mapper=MapperConfig.from_dict(data["mapper"]),
        )


@dataclass
class PipelineState:
    raw: dict[str, Any]
    derived: dict[str, Any] = field(default_factory=dict)
    context: dict[str, Any] = field(default_factory=dict)
