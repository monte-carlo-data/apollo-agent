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
        if not isinstance(data["type"], str):
            raise ValueError("TransformStep 'type' must be a str")
        if not isinstance(data["input"], dict):
            raise ValueError("TransformStep 'input' must be a dict")
        if not isinstance(data["output"], dict):
            raise ValueError("TransformStep 'output' must be a dict")
        field_map = data.get("field_map", {})
        if not isinstance(field_map, dict):
            raise ValueError("TransformStep 'field_map' must be a dict")
        return cls(
            type=data["type"],
            input=data["input"],
            output=data["output"],
            when=data.get("when"),
            field_map=field_map,
        )


@dataclass
class MapperConfig:
    field_map: dict[str, Any]
    name: str = ""
    schema: type | None = None
    passthrough: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MapperConfig:
        if "field_map" in data:
            # Full format: {field_map: {...}, name?: "...", passthrough?: bool}
            field_map = data["field_map"]
            name = data.get("name", "")
            passthrough = data.get("passthrough", False)
        else:
            # Shorthand: the entire dict is the field_map (no name or passthrough)
            field_map = data
            name = ""
            passthrough = False
        if not isinstance(field_map, dict):
            raise ValueError("MapperConfig 'field_map' must be a dict")
        return cls(
            field_map=field_map,
            name=name,
            # schema is always None when deserializing from JSON —
            # it is injected at runtime from the registered CTP for the connection type
            schema=None,
            passthrough=passthrough,
        )


@dataclass
class CtpConfig:
    mapper: MapperConfig
    name: str = ""
    steps: list[TransformStep] = field(default_factory=list)
    connect_args_defaults: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CtpConfig:
        if "mapper" not in data:
            raise ValueError("CtpConfig missing required field: 'mapper'")
        steps = data.get("steps", [])
        if not isinstance(steps, list):
            raise ValueError("CtpConfig 'steps' must be a list")
        if not all(isinstance(s, dict) for s in steps):
            raise ValueError("CtpConfig 'steps' must be a list of dicts")
        connect_args_defaults = data.get("connect_args_defaults", {})
        if not isinstance(connect_args_defaults, dict):
            raise ValueError("CtpConfig 'connect_args_defaults' must be a dict")
        return cls(
            mapper=MapperConfig.from_dict(data["mapper"]),
            name=data.get("name", ""),
            steps=[TransformStep.from_dict(s) for s in steps],
            connect_args_defaults=connect_args_defaults,
        )


@dataclass
class PipelineState:
    raw: dict[str, Any]
    derived: dict[str, Any] = field(default_factory=dict)
    context: dict[str, Any] = field(default_factory=dict)
