"""Support for setting arbitrary agent env vars from IaC via a single JSON var.

CloudFormation has no map/dict parameter type, so the agent accepts a JSON
object string in ``MCD_ADDITIONAL_ENV_VARS`` and injects each entry into
``os.environ`` at startup. Terraform feeds the same variable (a native
``map(string)`` run through ``jsonencode``), so both deploy paths converge on the
same env var and the same code path here.

This lets new agent toggles (e.g. ``MCD_ORACLE_THICK_MODE``) be enabled from IaC
without adding a new parameter to every module/template each time.
"""

import json
import logging
import os
from typing import Any, Dict, Optional

from apollo.common.agent.constants import ATTRIBUTE_VALUE_REDACTED

# Re-exported: agent-common applies the same predicate to its own health output,
# so the substring list lives in agent-base rather than in both repos.
from apollo.common.agent.redact import is_sensitive_env_var_name

logger = logging.getLogger(__name__)

ADDITIONAL_ENV_VARS_ENV_VAR = "MCD_ADDITIONAL_ENV_VARS"

__all__ = [
    "ADDITIONAL_ENV_VARS_ENV_VAR",
    "apply_additional_env_vars",
    "is_sensitive_env_var_name",
    "sanitized_blob_for_health",
]


def _parse_blob(raw: str) -> Optional[Dict[str, Any]]:
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        logger.warning("Ignoring %s: not valid JSON", ADDITIONAL_ENV_VARS_ENV_VAR)
        return None
    if not isinstance(parsed, dict):
        logger.warning(
            "Ignoring %s: expected a JSON object", ADDITIONAL_ENV_VARS_ENV_VAR
        )
        return None
    return parsed


def apply_additional_env_vars() -> None:
    """Inject entries from ``MCD_ADDITIONAL_ENV_VARS`` into ``os.environ``.

    Idempotent and never raises — it must not break agent startup. An env var
    already set explicitly takes precedence and is not overwritten. Values must
    be scalars; non-scalar entries are skipped with a warning. Only the applied
    keys are logged (never values, which may be sensitive).
    """
    raw = os.getenv(ADDITIONAL_ENV_VARS_ENV_VAR)
    if not raw:
        return
    parsed = _parse_blob(raw)
    if parsed is None:
        return

    applied = []
    for key, value in parsed.items():
        if not isinstance(key, str) or key in os.environ:
            # Skip malformed keys and never override an explicitly-set env var.
            continue
        if isinstance(value, bool):
            str_value = "true" if value else "false"
        elif isinstance(value, (str, int, float)):
            str_value = str(value)
        else:
            logger.warning(
                "Skipping %s entry %r: value must be a scalar",
                ADDITIONAL_ENV_VARS_ENV_VAR,
                key,
            )
            continue
        os.environ[key] = str_value
        applied.append(key)

    if applied:
        logger.info(
            "Applied %d additional env var(s) from %s: %s",
            len(applied),
            ADDITIONAL_ENV_VARS_ENV_VAR,
            ", ".join(sorted(applied)),
        )


def sanitized_blob_for_health(raw: str) -> str:
    """Return the blob for health display with sensitive-named entries redacted.

    The per-key sensitivity filter that guards individual env vars in health
    can't protect a secret nested inside this single (non-sensitive-named) blob,
    so redact the value of any sensitive-named key here. Invalid input is
    replaced with a diagnostic placeholder so parsing issues are visible without
    exposing raw content.
    """
    parsed = _parse_blob(raw)
    if parsed is None:
        return "<invalid: expected a JSON object>"
    return json.dumps(
        {
            key: (ATTRIBUTE_VALUE_REDACTED if is_sensitive_env_var_name(key) else value)
            for key, value in parsed.items()
        }
    )
