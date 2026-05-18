import os
from unittest import TestCase
from unittest.mock import patch

import azure.functions as func

from apollo.interfaces.azure.auth import resolve_auth_level

# Full set of Easy Auth env vars as the platform would inject.
_EASY_AUTH_ENV = {
    "WEBSITE_AUTH_ENABLED": "True",
    "WEBSITE_AUTH_CLIENT_ID": "00000000-0000-0000-0000-000000000000",
    "WEBSITE_AUTH_OPENID_ISSUER": "https://login.microsoftonline.com/tenant-id",
}


class TestAzureAuthLevel(TestCase):
    """Tests for MCD_AUTH_TYPE-driven auth level selection in the Azure Function App."""

    def test_auth_level_default_when_env_var_not_set(self):
        """Without MCD_AUTH_TYPE, auth level should be FUNCTION."""
        with patch.dict(os.environ, {}, clear=True):
            assert resolve_auth_level() == func.AuthLevel.FUNCTION

    def test_auth_level_function_when_app_key(self):
        """With MCD_AUTH_TYPE=AZURE_FUNCTION_APP_KEY, auth level should be FUNCTION."""
        with patch.dict(
            os.environ, {"MCD_AUTH_TYPE": "AZURE_FUNCTION_APP_KEY"}, clear=True
        ):
            assert resolve_auth_level() == func.AuthLevel.FUNCTION

    def test_auth_level_function_for_unknown_value(self):
        """An unrecognized MCD_AUTH_TYPE value should default to FUNCTION."""
        with patch.dict(os.environ, {"MCD_AUTH_TYPE": "SOMETHING_ELSE"}, clear=True):
            assert resolve_auth_level() == func.AuthLevel.FUNCTION

    def test_auth_level_anonymous_when_service_principal_and_easy_auth(self):
        """SP auth + all Easy Auth vars present -> ANONYMOUS."""
        with patch.dict(
            os.environ,
            {"MCD_AUTH_TYPE": "AZURE_FUNCTION_SERVICE_PRINCIPAL", **_EASY_AUTH_ENV},
            clear=True,
        ):
            assert resolve_auth_level() == func.AuthLevel.ANONYMOUS

    def test_raises_when_service_principal_but_no_easy_auth(self):
        """SP auth requested but no Easy Auth vars -> RuntimeError (fail closed)."""
        with patch.dict(
            os.environ,
            {"MCD_AUTH_TYPE": "AZURE_FUNCTION_SERVICE_PRINCIPAL"},
            clear=True,
        ):
            with self.assertRaises(RuntimeError) as ctx:
                resolve_auth_level()
            assert "WEBSITE_AUTH_ENABLED" in str(ctx.exception)
            assert "WEBSITE_AUTH_CLIENT_ID" in str(ctx.exception)
            assert "WEBSITE_AUTH_OPENID_ISSUER" in str(ctx.exception)

    def test_raises_when_service_principal_and_partial_easy_auth(self):
        """SP auth + only WEBSITE_AUTH_ENABLED -> RuntimeError listing missing vars."""
        with patch.dict(
            os.environ,
            {
                "MCD_AUTH_TYPE": "AZURE_FUNCTION_SERVICE_PRINCIPAL",
                "WEBSITE_AUTH_ENABLED": "True",
            },
            clear=True,
        ):
            with self.assertRaises(RuntimeError) as ctx:
                resolve_auth_level()
            # Only the two missing vars should be listed
            assert "WEBSITE_AUTH_ENABLED" not in str(ctx.exception)
            assert "WEBSITE_AUTH_CLIENT_ID" in str(ctx.exception)
            assert "WEBSITE_AUTH_OPENID_ISSUER" in str(ctx.exception)

    def test_raises_when_auth_enabled_is_false(self):
        """SP auth + WEBSITE_AUTH_ENABLED=False -> RuntimeError (value must be True)."""
        with patch.dict(
            os.environ,
            {
                "MCD_AUTH_TYPE": "AZURE_FUNCTION_SERVICE_PRINCIPAL",
                "WEBSITE_AUTH_ENABLED": "False",
                "WEBSITE_AUTH_CLIENT_ID": "some-client-id",
                "WEBSITE_AUTH_OPENID_ISSUER": "https://login.microsoftonline.com/tid",
            },
            clear=True,
        ):
            with self.assertRaises(RuntimeError) as ctx:
                resolve_auth_level()
            assert "WEBSITE_AUTH_ENABLED" in str(ctx.exception)
            # The other two are present, so they shouldn't be listed
            assert "WEBSITE_AUTH_CLIENT_ID" not in str(ctx.exception)
            assert "WEBSITE_AUTH_OPENID_ISSUER" not in str(ctx.exception)

    def test_raises_when_service_principal_missing_only_issuer(self):
        """SP auth + ENABLED + CLIENT_ID but no ISSUER -> RuntimeError."""
        with patch.dict(
            os.environ,
            {
                "MCD_AUTH_TYPE": "AZURE_FUNCTION_SERVICE_PRINCIPAL",
                "WEBSITE_AUTH_ENABLED": "True",
                "WEBSITE_AUTH_CLIENT_ID": "some-client-id",
            },
            clear=True,
        ):
            with self.assertRaises(RuntimeError) as ctx:
                resolve_auth_level()
            assert "WEBSITE_AUTH_OPENID_ISSUER" in str(ctx.exception)
            assert "WEBSITE_AUTH_CLIENT_ID" not in str(ctx.exception)
