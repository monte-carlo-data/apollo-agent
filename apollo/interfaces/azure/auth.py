"""Resolve Azure Function auth level based on Easy Auth configuration.

Extracted from function_app.py so the logic can be imported in tests
without triggering module-level side effects (Azure Monitor, logging setup).

Also provides runtime verification that Easy Auth is actually intercepting
unauthenticated requests (lazy self-call probe).
"""

import logging
import os
import secrets
import time
from typing import Any, Dict, Optional, Tuple

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

# Per-process random token ensures only this process can trigger the probe
# short-circuit.  Without this, any caller who knows the header name could
# bypass Easy Auth verification — exactly the failure mode this feature
# is designed to detect.
_EASY_AUTH_PROBE_TOKEN = secrets.token_hex(32)

# The check-then-act on this boolean is not atomic, but Azure Functions
# workers typically use a single-threaded WSGI server so concurrent health
# requests within one process are unlikely.  At worst, a few redundant
# probe requests fire during cold start — no data corruption risk.
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
    """Return True if the request carries a valid Easy Auth probe token.

    Only the current process knows the token value, so external callers
    cannot forge a probe request to bypass verification.
    """
    return headers.get(_EASY_AUTH_PROBE_HEADER) == _EASY_AUTH_PROBE_TOKEN


def verify_easy_auth_enforcement() -> Optional[str]:
    """Verify that Easy Auth is actually rejecting unauthenticated requests.

    Sends an unauthenticated probe to the health endpoint and expects a
    401/403 from the Easy Auth middleware.  Successful verification is
    cached for the process lifetime.  Failed results are not cached and
    will be re-checked on each call.

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
                headers={_EASY_AUTH_PROBE_HEADER: _EASY_AUTH_PROBE_TOKEN},
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
            # Unexpected/transient status (3xx, 4xx other than 401/403,
            # 5xx) — retry like connection errors.  Platform 502/503 is
            # common during Azure Function cold-start.
            last_error = Exception(  # noqa: TRY002
                f"Unexpected status code from Easy Auth enforcement "
                f"probe: {resp.status_code}"
            )
            logger.warning(
                f"Easy Auth probe got {resp.status_code} — will retry "
                f"({attempt + 1}/3)"
            )
            if attempt < 2:
                time.sleep(1)
            continue
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < 2:
                time.sleep(1)
            continue

    msg = f"Could not verify Easy Auth enforcement after 3 attempts: {last_error}"
    logger.error(msg)
    return msg


# ---------------------------------------------------------------------------
# Health endpoint hooks — registered by azure/main.py via
# generic.main.register_health_hooks() when SP auth is active.
# ---------------------------------------------------------------------------


def easy_auth_pre_health_hook(headers: Any) -> Optional[Tuple[Dict, int]]:
    """Short-circuit probe requests to avoid infinite recursion.

    The verification probe includes a per-process token.  When we see it,
    return a minimal 200 immediately — the probe sender only cares whether
    the request was rejected by Easy Auth (401/403) or reached our code.
    """
    if is_easy_auth_probe(headers):
        return {"status": "up"}, 200
    return None


def easy_auth_post_health_hook(health_dict: Dict) -> Optional[Tuple[Dict, int]]:
    """Verify Easy Auth enforcement after building the health response.

    Returns a 503 override if verification fails, causing the DC's
    reachability check to fail and blocking the deployment.
    """
    error = verify_easy_auth_enforcement()
    if error:
        logger.error(f"Easy Auth enforcement check failed: {error}")
        health_dict["easy_auth_error"] = "Easy Auth enforcement could not be verified"
        return health_dict, 503
    return None
