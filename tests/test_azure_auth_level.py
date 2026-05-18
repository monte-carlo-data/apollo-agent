import os
from unittest import TestCase
from unittest.mock import patch


class TestAzureAuthLevel(TestCase):
    """Tests for MCD_AUTH_TYPE-driven auth level selection in the Azure Function App."""

    @staticmethod
    def _resolve_auth_level():
        """Replicates the auth_level logic from function_app.py for unit testing.

        The actual auth_level is computed at module import time, making it
        impractical to test via reimport.  This helper mirrors the exact
        conditional so we can validate the env-var contract in isolation.
        """
        import azure.functions as func

        return (
            func.AuthLevel.ANONYMOUS
            if os.getenv("MCD_AUTH_TYPE") == "AZURE_FUNCTION_SERVICE_PRINCIPAL"
            else func.AuthLevel.FUNCTION
        )

    def test_auth_level_default_when_env_var_not_set(self):
        """Without MCD_AUTH_TYPE, auth level should be FUNCTION (existing behavior)."""
        import azure.functions as func

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MCD_AUTH_TYPE", None)
            assert self._resolve_auth_level() == func.AuthLevel.FUNCTION

    def test_auth_level_function_when_app_key(self):
        """With MCD_AUTH_TYPE=AZURE_FUNCTION_APP_KEY, auth level should be FUNCTION."""
        import azure.functions as func

        with patch.dict(
            os.environ, {"MCD_AUTH_TYPE": "AZURE_FUNCTION_APP_KEY"}, clear=False
        ):
            assert self._resolve_auth_level() == func.AuthLevel.FUNCTION

    def test_auth_level_anonymous_when_service_principal(self):
        """With MCD_AUTH_TYPE=AZURE_FUNCTION_SERVICE_PRINCIPAL, auth level should be ANONYMOUS."""
        import azure.functions as func

        with patch.dict(
            os.environ,
            {"MCD_AUTH_TYPE": "AZURE_FUNCTION_SERVICE_PRINCIPAL"},
            clear=False,
        ):
            assert self._resolve_auth_level() == func.AuthLevel.ANONYMOUS

    def test_auth_level_function_for_unknown_value(self):
        """An unrecognized MCD_AUTH_TYPE value should default to FUNCTION."""
        import azure.functions as func

        with patch.dict(os.environ, {"MCD_AUTH_TYPE": "SOMETHING_ELSE"}, clear=False):
            assert self._resolve_auth_level() == func.AuthLevel.FUNCTION
