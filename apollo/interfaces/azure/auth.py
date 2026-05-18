"""Resolve Azure Function auth level based on Easy Auth configuration.

Extracted from function_app.py so the logic can be imported in tests
without triggering module-level side effects (Azure Monitor, logging setup).
"""

import os

import azure.functions as func

# Presence-only check: these must be set (non-empty) for Easy Auth to be
# considered configured.
_EASY_AUTH_PRESENCE_VARS = (
    "WEBSITE_AUTH_CLIENT_ID",
    "WEBSITE_AUTH_OPENID_ISSUER",
)


def resolve_auth_level() -> func.AuthLevel:
    """Determine the appropriate auth level for the Azure Function App.

    When MCD_AUTH_TYPE is set to AZURE_FUNCTION_SERVICE_PRINCIPAL, validates
    that Easy Auth is actually enabled by checking platform-injected env vars.
    Raises RuntimeError at startup if Easy Auth is not configured — fail-closed.
    """
    if os.getenv("MCD_AUTH_TYPE") != "AZURE_FUNCTION_SERVICE_PRINCIPAL":
        return func.AuthLevel.FUNCTION

    # WEBSITE_AUTH_ENABLED must be explicitly "True" (case-insensitive);
    # CLIENT_ID and OPENID_ISSUER just need to be present.
    missing = [v for v in _EASY_AUTH_PRESENCE_VARS if not os.getenv(v)]
    if os.getenv("WEBSITE_AUTH_ENABLED", "").lower() != "true":
        missing.insert(0, "WEBSITE_AUTH_ENABLED")
    if missing:
        raise RuntimeError(
            "MCD_AUTH_TYPE is set to AZURE_FUNCTION_SERVICE_PRINCIPAL but Easy "
            "Auth does not appear to be enabled — the following platform "
            f"environment variables are missing or invalid: {', '.join(missing)}. "
            "Refusing to start with AuthLevel.ANONYMOUS because the function "
            "would be unauthenticated. Enable Easy Auth on the Function App "
            "or switch to Function Key authentication."
        )

    return func.AuthLevel.ANONYMOUS
