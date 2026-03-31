# apollo/integrations/ctp/mapper.py
import typing
from typing import Any

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import MapperConfig, PipelineState
from apollo.integrations.ctp.template import TemplateEngine

# Types that support safe coercion from a mismatched primitive value.
_COERCIBLE_TYPES = (int, float, str)


def _coerce(value: Any, annotation: Any) -> Any:
    """Coerce value to the type described by a TypedDict annotation.

    Unwraps Required[T] / NotRequired[T], then attempts coercion only for
    simple built-in types (int, float, str).  Any, Union, and complex
    generics are left unchanged.  bool is excluded intentionally because
    bool("false") == True, which is surprising; NativeEnvironment handles
    boolean templates correctly without coercion.
    """
    origin = typing.get_origin(annotation)
    if origin is typing.Required or origin is typing.NotRequired:
        annotation = typing.get_args(annotation)[0]
        origin = typing.get_origin(annotation)

    # Skip Any and parameterised generics (Union, Optional, list[…], etc.)
    if annotation is typing.Any or origin is not None:
        return value

    if annotation not in _COERCIBLE_TYPES:
        return value

    if type(value) is annotation:  # already exact type — includes bool vs int
        return value

    try:
        return annotation(value)
    except (ValueError, TypeError):
        return value


class Mapper:
    def execute(
        self,
        config: MapperConfig,
        state: PipelineState,
        step_field_maps: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if config.passthrough:
            return dict(state.raw)

        # Base field_map first, then step contributions (step takes precedence on collision)
        combined = {**config.field_map, **(step_field_maps or {})}
        result = {}
        for key, template in combined.items():
            value = (
                TemplateEngine.render(template, state)
                if isinstance(template, str)
                else template
            )
            if value is not None:
                result[key] = value

        if config.schema is not None:
            missing = config.schema.__required_keys__ - result.keys()
            if missing:
                raise CtpPipelineError(
                    stage="mapper_validation",
                    message=f"Missing required fields: {sorted(missing)}",
                )

            # Coerce known fields to their declared types; unknown fields pass through.
            schema_annotations = typing.get_type_hints(
                config.schema, include_extras=True
            )
            result = {
                key: (
                    _coerce(value, schema_annotations[key])
                    if key in schema_annotations
                    else value
                )
                for key, value in result.items()
            }

        return result
