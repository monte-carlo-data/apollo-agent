"""Resolve Azure Function auth level based on Easy Auth configuration.

Extracted from function_app.py so the logic can be imported in tests
without triggering module-level side effects (Azure Monitor, logging setup).

Also provides runtime verification that Easy Auth is actually intercepting
unauthenticated requests (lazy self-call probe).
"""

import logging
import os
import time
from typing import Any, Optional

import azure.functions as func
import requests

logger = logging.getLogger(__name__)

# Presence-only check: these must be set (non-empty) for Easy Auth to be
# considered configured.
_EASY_AUTH_PRESENCE_VARS = (
    "WEBSITE_AUTH_CLIENT_ID",
    "WEBSITE_AUTH_OPENID_ISSUER",
)

_EASY_AUTH_PROBE_HEADER = "X-MCD-EasyAuth-Probe"

_easy_auth_verified = False


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


def is_easy_auth_probe(headers: Any) -> bool:
    """Return True if the request carries the Easy Auth probe header."""
    return _EASY_AUTH_PROBE_HEADER in headers


def verify_easy_auth_enforcement() -> Optional[str]:
    """Verify that Easy Auth is actually rejecting unauthenticated requests.

    Sends an unauthenticated probe to the health endpoint and expects a
    401/403 from the Easy Auth middleware.  The result is cached so the
    probe only runs once per process lifetime.

    Returns:
        None if enforcement is verified (or was already cached).
        An error message string if verification fails.
    """
    global _easy_auth_verified

    if _easy_auth_verified:
        return None

    hostname = os.getenv("WEBSITE_HOSTNAME")
    if not hostname:
        return "WEBSITE_HOSTNAME not set — cannot verify Easy Auth enforcement"

    url = f"https://{hostname}/api/v1/test/health"
    last_error: Optional[Exception] = None

    for attempt in range(3):
        try:
            resp = requests.get(
                url,
                headers={_EASY_AUTH_PROBE_HEADER: "1"},
                timeout=10,
            )
            if resp.status_code in (401, 403):
                _easy_auth_verified = True
                logger.info(
                    "Easy Auth enforcement verified — unauthenticated "
                    "request was rejected"
                )
                return None
            if 200 <= resp.status_code <= 299:
                msg = (
                    "Easy Auth is NOT intercepting unauthenticated "
                    f"requests — health probe returned {resp.status_code}"
                )
                logger.critical(msg)
                return msg
            # Unexpected status code (3xx, 4xx other than 401/403, 5xx)
            msg = (
                "Unexpected status code from Easy Auth enforcement "
                f"probe: {resp.status_code}"
            )
            logger.error(msg)
            return msg
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < 2:
                time.sleep(1)
            continue

    msg = "Could not verify Easy Auth enforcement after 3 attempts: " f"{last_error}"
    logger.error(msg)
    return msg
