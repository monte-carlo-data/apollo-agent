"""Validator entry point for self-hosted credentials.

Runs cerberus against the decoded credentials dict and returns its native
errors dict. ``allow_unknown=True`` is set at the root so customers can
include forward-compatible fields without spurious errors — the
integration's CTP or proxy client silently ignores unknown fields anyway.

Multi-auth-mode connectors express their "at most one of these auth modes
may be present" constraint inside the cerberus schema via ``oneof_schema``
(see ``apollo/integrations/ctp/defaults/snowflake.py`` for the canonical
example). No additional check layer lives here — cerberus alone covers
every constraint we need.
"""

from __future__ import annotations

from typing import Any, cast

# Cerberus does not ship type stubs; pyright sees ``Validator`` as zero-arg
# with no methods. Rebinding through ``Any`` keeps every call site clean
# without per-line ``pyright: ignore`` comments. The library is stable and
# widely used (and is what the data-collector uses internally), so erasing
# the type is correct rather than a workaround.
from cerberus import Validator as _CerberusValidator  # type: ignore[import-untyped]

Validator: Any = _CerberusValidator


def validate(
    cerberus_schema: dict[str, Any], credentials: dict[str, Any]
) -> dict[str, Any]:
    """Validate ``credentials`` against ``cerberus_schema``.

    Returns cerberus's native ``Validator.errors`` dict — empty iff
    validation succeeds. Non-dict input is rejected up-front so the route
    does not have to do its own type check.
    """
    if not isinstance(credentials, dict):
        return {
            "__root__": [
                f"credentials must be a JSON object, got {type(credentials).__name__}"
            ]
        }

    cerberus_validator = Validator(cerberus_schema, allow_unknown=True)
    cerberus_validator.validate(credentials)
    # Cerberus's stubs type ``.errors`` as a union including a bytes-keyed
    # variant used internally for some constraint types. At the top level
    # the dict is always keyed by field names (strings), so we narrow with
    # an explicit cast — the assignment alone is not enough for IDEs that
    # check RHS types against LHS annotations.
    return cast("dict[str, Any]", dict(cerberus_validator.errors))
