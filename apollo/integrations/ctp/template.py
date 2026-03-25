# apollo/integrations/ctp/template.py
from typing import Any

from jinja2 import StrictUndefined, Undefined
from jinja2.nativetypes import NativeEnvironment
from jinja2.sandbox import SandboxedEnvironment

from apollo.integrations.ctp.models import PipelineState


class _CredentialNamespace(dict):
    """Marker base class for CTP credential namespaces (raw, derived, context).

    Used by _NativeSandboxedEnvironment.is_safe_attribute to allow single-underscore
    attribute access (e.g. raw._user_agent_entry) while still blocking dunder access.
    Credential field names may legitimately start with _ and must be accessible in
    templates via dot notation.
    """


class _NativeSandboxedEnvironment(SandboxedEnvironment, NativeEnvironment):
    """Jinja2 environment combining sandbox safety with native type preservation.

    SandboxedEnvironment (first in MRO) blocks access to unsafe Python attributes
    and builtins inside template expressions — defense-in-depth against malicious
    template strings in CTP configs.

    NativeEnvironment ensures rendered values preserve their Python type (int stays
    int, bool stays bool) rather than being coerced to strings.

    Variable *values* (e.g. raw.password) are always treated as data by Jinja2 and
    are never re-rendered, regardless of whether they contain {{ }} characters.
    """

    def is_safe_attribute(self, obj: object, attr: str, value: object) -> bool:
        # Allow single-underscore attribute access on our controlled credential
        # namespaces. The sandbox blocks all _-prefixed attrs by default, but
        # credential field names (e.g. _user_agent_entry) may start with _.
        # Dunder access is always blocked regardless.
        if isinstance(obj, _CredentialNamespace) and not (
            attr.startswith("__") and attr.endswith("__")
        ):
            return True
        return super().is_safe_attribute(obj, attr, value)


_ENV = _NativeSandboxedEnvironment(undefined=StrictUndefined)


class _StrictDict(_CredentialNamespace):
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
        Template strings are rendered using _NativeSandboxedEnvironment so the
        returned type matches the actual Python type (int, bool, None, etc.)
        while unsafe attribute/builtin access in templates is blocked.
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
