# apollo/integrations/ctp/template.py
from typing import Any

from jinja2 import StrictUndefined, Undefined
from jinja2.nativetypes import NativeEnvironment

from apollo.integrations.ctp.models import PipelineState

_ENV = NativeEnvironment(undefined=StrictUndefined)


class _StrictDict(dict):
    """Dict subclass whose attribute access returns Undefined (not raises) so
    Jinja2 filters like ``default()`` and tests like ``is defined`` work
    correctly, while bare references to missing keys ultimately raise
    UndefinedError after render() post-processing.

    Note: soft-Undefined behavior applies only to attribute (dot-notation) access
    (e.g. ``raw.key``). Subscript access (e.g. ``raw['key']``) bypasses this and
    will raise immediately if the key is missing — don't combine bracket notation
    with ``default()`` or ``is defined`` in templates.
    """

    def __getattr__(self, key: str) -> Any:
        if key.startswith("__") and key.endswith("__"):
            raise AttributeError(key)
        try:
            return self[key]
        except KeyError:
            return _ENV.undefined(f"{key!r} is undefined", name=key)


class TemplateEngine:
    @classmethod
    def render(cls, value: Any, state: PipelineState) -> Any:
        """
        Render a value against pipeline state.
        Non-string values are returned as-is.
        Strings without Jinja2 markers are returned as-is.
        Template strings are rendered using NativeEnvironment so the returned
        type matches the actual Python type (int, bool, None, etc.).
        """
        if not isinstance(value, str):
            return value
        if "{{" not in value and "{%" not in value:
            return value
        template = _ENV.from_string(value)
        result = template.render(
            raw=_StrictDict(state.raw),
            derived=_StrictDict(state.derived),
            context=_StrictDict(state.context),
        )
        # NativeEnvironment returns Undefined objects as-is rather than
        # stringifying them (which would trigger StrictUndefined.__str__).
        # Explicitly fail here so callers see UndefinedError for missing refs.
        if isinstance(result, Undefined):
            result._fail_with_undefined_error()
        return result

    @classmethod
    def evaluate_condition(cls, expression: str, state: PipelineState) -> bool:
        """Evaluate a Jinja2 boolean condition expression (no braces needed)."""
        template = _ENV.from_string(
            f"{{% if {expression} %}}True{{% else %}}False{{% endif %}}"
        )
        result = template.render(
            raw=_StrictDict(state.raw),
            derived=_StrictDict(state.derived),
            context=_StrictDict(state.context),
        )
        # NativeEnvironment evaluates `True`/`False` identifiers as Python bool literals,
        # so result is genuinely bool True/False (not the strings "True"/"False").
        return bool(result)
