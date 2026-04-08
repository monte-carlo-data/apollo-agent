# apollo/integrations/ctp/validator.py
"""Structural validation for custom CTP configs.

Validates a caller-supplied CtpConfig dict without executing the pipeline:
  - Deserializes via CtpConfig.from_dict()
  - Injects the TypedDict schema from the registered CTP for the connection type
  - Verifies that all transform step types are registered
  - Checks Jinja2 template syntax in field_map values and step when-expressions
  - Confirms the mapper field_map covers all TypedDict required keys
"""
from typing import Any

from jinja2 import Environment, TemplateSyntaxError

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import CtpConfig
from apollo.integrations.ctp.registry import CtpRegistry
from apollo.integrations.ctp.transforms.registry import TransformRegistry

_syntax_env = Environment()


def _check_template_syntax(template: str, location: str) -> str | None:
    try:
        _syntax_env.parse(template)
        return None
    except TemplateSyntaxError as e:
        return f"Template syntax error in {location}: {e.message}"


def validate_ctp_config(
    connection_type: str,
    ctp_config: dict[str, Any],
) -> dict[str, Any]:
    """Validate a custom CTP config dict structurally without executing the pipeline.

    Returns ``{"valid": True, "errors": []}`` on success, or
    ``{"valid": False, "errors": [...]}`` with one entry per problem found.
    """
    errors: list[str] = []

    # 1. Deserialize
    try:
        config = CtpConfig.from_dict(ctp_config)
    except ValueError as e:
        return {"valid": False, "errors": [str(e)]}

    # 2. Inject schema from the registered CTP so required-key coverage can be checked
    registered = CtpRegistry.get(connection_type)
    if registered is not None:
        config.mapper.schema = registered.mapper.schema

    # 3. Verify all transform step types are registered
    for step in config.steps:
        try:
            TransformRegistry.get(step.type)
        except CtpPipelineError as e:
            errors.append(str(e))

    # 4. Check Jinja2 template syntax in mapper field_map values
    for key, template in config.mapper.field_map.items():
        if isinstance(template, str) and ("{{" in template or "{%" in template):
            err = _check_template_syntax(template, f"mapper.field_map[{key!r}]")
            if err:
                errors.append(err)

    # Check Jinja2 syntax in step field_map values and when-expressions
    for step in config.steps:
        if step.when:
            when_template = f"{{% if {step.when} %}}True{{% else %}}False{{% endif %}}"
            err = _check_template_syntax(when_template, f"step '{step.type}' when")
            if err:
                errors.append(err)
        for key, template in step.field_map.items():
            if isinstance(template, str) and ("{{" in template or "{%" in template):
                err = _check_template_syntax(
                    template, f"step '{step.type}' field_map[{key!r}]"
                )
                if err:
                    errors.append(err)

    # 5. Schema coverage: mapper field_map must cover all TypedDict required keys.
    # If steps are present they may also contribute keys, so missing keys are a
    # warning rather than a hard failure in that case.
    if config.mapper.schema is not None:
        required = config.mapper.schema.__required_keys__
        provided = set(config.mapper.field_map.keys())
        missing = required - provided
        if missing:
            if config.steps:
                errors.append(
                    f"Mapper field_map may be missing required keys "
                    f"(steps may provide them): {sorted(missing)}"
                )
            else:
                errors.append(
                    f"Mapper field_map is missing required keys: {sorted(missing)}"
                )

    return {"valid": len(errors) == 0, "errors": errors}
