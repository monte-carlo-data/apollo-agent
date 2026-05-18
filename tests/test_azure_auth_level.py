import os
from unittest import TestCase
from unittest.mock import patch

import azure.functions as func


# Mirrors _EASY_AUTH_REQUIRED_VARS from function_app.py
_EASY_AUTH_REQUIRED_VARS = (
    "WEBSITE_AUTH_ENABLED",
    "WEBSITE_AUTH_CLIENT_ID",
    "WEBSITE_AUTH_OPENID_ISSUER",
)

# Convenience: a full set of Easy Auth env vars as the platform would inject.
_EASY_AUTH_ENV = {
    "WEBSITE_AUTH_ENABLED": "True",
    "WEBSITE_AUTH_CLIENT_ID": "00000000-0000-0000-0000-000000000000",
    "WEBSITE_AUTH_OPENID_ISSUER": "https://login.microsoftonline.com/tenant-id",
}


def _resolve_auth_level() -> func.AuthLevel:
    """Mirrors _resolve_auth_level from function_app.py for unit testing.

    The actual function is computed at module import time, making it
    impractical to test via reimport.  This helper mirrors the exact
    logic so we can validate the env-var contract in isolation.
    """
    if os.getenv("MCD_AUTH_TYPE") != "AZURE_FUNCTION_SERVICE_PRINCIPAL":
        return func.AuthLevel.FUNCTION

    missing = [v for v in _EASY_AUTH_REQUIRED_VARS if not os.getenv(v)]
    if os.getenv("WEBSITE_AUTH_ENABLED", "").lower() != "true":
        missing.append("WEBSITE_AUTH_ENABLED")
    missing = list(dict.fromkeys(missing))
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


def _clean_env():
    """Remove all auth-related env vars so tests start from a known state."""
    for var in ("MCD_AUTH_TYPE", *_EASY_AUTH_REQUIRED_VARS):
        os.environ.pop(var, None)


class TestAzureAuthLevel(TestCase):
    """Tests for MCD_AUTH_TYPE-driven auth level selection in the Azure Function App."""

    def test_auth_level_default_when_env_var_not_set(self):
        """Without MCD_AUTH_TYPE, auth level should be FUNCTION."""
        with patch.dict(os.environ, {}, clear=False):
            _clean_env()
            assert _resolve_auth_level() == func.AuthLevel.FUNCTION

    def test_auth_level_function_when_app_key(self):
        """With MCD_AUTH_TYPE=AZURE_FUNCTION_APP_KEY, auth level should be FUNCTION."""
        with patch.dict(
            os.environ, {"MCD_AUTH_TYPE": "AZURE_FUNCTION_APP_KEY"}, clear=False
        ):
            assert _resolve_auth_level() == func.AuthLevel.FUNCTION

    def test_auth_level_function_for_unknown_value(self):
        """An unrecognized MCD_AUTH_TYPE value should default to FUNCTION."""
        with patch.dict(os.environ, {"MCD_AUTH_TYPE": "SOMETHING_ELSE"}, clear=False):
            assert _resolve_auth_level() == func.AuthLevel.FUNCTION

    def test_auth_level_anonymous_when_service_principal_and_easy_auth(self):
        """SP auth + all Easy Auth vars present → ANONYMOUS."""
        with patch.dict(
            os.environ,
            {"MCD_AUTH_TYPE": "AZURE_FUNCTION_SERVICE_PRINCIPAL", **_EASY_AUTH_ENV},
            clear=False,
        ):
            assert _resolve_auth_level() == func.AuthLevel.ANONYMOUS

    def test_raises_when_service_principal_but_no_easy_auth(self):
        """SP auth requested but no Easy Auth vars → RuntimeError (fail closed)."""
        with patch.dict(os.environ, {}, clear=False):
            _clean_env()
            os.environ["MCD_AUTH_TYPE"] = "AZURE_FUNCTION_SERVICE_PRINCIPAL"
            with self.assertRaises(RuntimeError) as ctx:
                _resolve_auth_level()
            assert "WEBSITE_AUTH_ENABLED" in str(ctx.exception)
            assert "WEBSITE_AUTH_CLIENT_ID" in str(ctx.exception)
            assert "WEBSITE_AUTH_OPENID_ISSUER" in str(ctx.exception)

    def test_raises_when_service_principal_and_partial_easy_auth(self):
        """SP auth + only WEBSITE_AUTH_ENABLED → RuntimeError listing missing vars."""
        with patch.dict(os.environ, {}, clear=False):
            _clean_env()
            os.environ["MCD_AUTH_TYPE"] = "AZURE_FUNCTION_SERVICE_PRINCIPAL"
            os.environ["WEBSITE_AUTH_ENABLED"] = "True"
            with self.assertRaises(RuntimeError) as ctx:
                _resolve_auth_level()
            # Only the two missing vars should be listed
            assert "WEBSITE_AUTH_ENABLED" not in str(ctx.exception)
            assert "WEBSITE_AUTH_CLIENT_ID" in str(ctx.exception)
            assert "WEBSITE_AUTH_OPENID_ISSUER" in str(ctx.exception)

    def test_raises_when_auth_enabled_is_false(self):
        """SP auth + WEBSITE_AUTH_ENABLED=False → RuntimeError (value must be True)."""
        env = {
            "MCD_AUTH_TYPE": "AZURE_FUNCTION_SERVICE_PRINCIPAL",
            "WEBSITE_AUTH_ENABLED": "False",
            "WEBSITE_AUTH_CLIENT_ID": "some-client-id",
            "WEBSITE_AUTH_OPENID_ISSUER": "https://login.microsoftonline.com/tid",
        }
        with patch.dict(os.environ, env, clear=False):
            with self.assertRaises(RuntimeError) as ctx:
                _resolve_auth_level()
            assert "WEBSITE_AUTH_ENABLED" in str(ctx.exception)
            # The other two are present, so they shouldn't be listed
            assert "WEBSITE_AUTH_CLIENT_ID" not in str(ctx.exception)
            assert "WEBSITE_AUTH_OPENID_ISSUER" not in str(ctx.exception)

    def test_raises_when_service_principal_missing_only_issuer(self):
        """SP auth + ENABLED + CLIENT_ID but no ISSUER → RuntimeError."""
        env = {
            "MCD_AUTH_TYPE": "AZURE_FUNCTION_SERVICE_PRINCIPAL",
            "WEBSITE_AUTH_ENABLED": "True",
            "WEBSITE_AUTH_CLIENT_ID": "some-client-id",
        }
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("WEBSITE_AUTH_OPENID_ISSUER", None)
            with self.assertRaises(RuntimeError) as ctx:
                _resolve_auth_level()
            assert "WEBSITE_AUTH_OPENID_ISSUER" in str(ctx.exception)
            assert "WEBSITE_AUTH_CLIENT_ID" not in str(ctx.exception)
