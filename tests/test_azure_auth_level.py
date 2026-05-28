import os
from unittest import TestCase
from unittest.mock import Mock, patch

import azure.functions as func

from apollo.interfaces.azure.auth import (
    _EASY_AUTH_PROBE_HEADER,
    _EASY_AUTH_PROBE_TOKEN,
    is_easy_auth_probe,
    resolve_auth_level,
    verify_easy_auth_enforcement,
)

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


class TestEasyAuthEnforcementVerification(TestCase):
    """Tests for the lazy self-call probe that verifies Easy Auth enforcement."""

    @patch("apollo.interfaces.azure.auth.requests.get")
    @patch.dict(
        os.environ, {"WEBSITE_HOSTNAME": "myfunc.azurewebsites.net"}, clear=True
    )
    def test_verify_returns_none_on_401(self, mock_get):
        """A 401 response means Easy Auth is blocking — verification passes."""
        mock_get.return_value = Mock(status_code=401)
        result = verify_easy_auth_enforcement()
        assert result is None

    @patch("apollo.interfaces.azure.auth.requests.get")
    @patch.dict(
        os.environ, {"WEBSITE_HOSTNAME": "myfunc.azurewebsites.net"}, clear=True
    )
    def test_verify_returns_none_on_403(self, mock_get):
        """A 403 response means Easy Auth is blocking — verification passes."""
        mock_get.return_value = Mock(status_code=403)
        result = verify_easy_auth_enforcement()
        assert result is None

    @patch("apollo.interfaces.azure.auth.requests.get")
    @patch.dict(
        os.environ, {"WEBSITE_HOSTNAME": "myfunc.azurewebsites.net"}, clear=True
    )
    def test_verify_returns_error_on_200(self, mock_get):
        """A 200 means the request went through unauthenticated — verification fails."""
        mock_get.return_value = Mock(status_code=200)
        result = verify_easy_auth_enforcement()
        assert result is not None
        assert "NOT intercepting" in result

    @patch("apollo.interfaces.azure.auth.time.sleep")
    @patch("apollo.interfaces.azure.auth.requests.get")
    @patch.dict(
        os.environ, {"WEBSITE_HOSTNAME": "myfunc.azurewebsites.net"}, clear=True
    )
    def test_verify_returns_error_on_unexpected_status(self, mock_get, mock_sleep):
        """An unexpected 500 is now retried — all 3 attempts return 500."""
        mock_get.return_value = Mock(status_code=500)
        result = verify_easy_auth_enforcement()
        assert result is not None
        assert "Unexpected status code" in result
        assert mock_get.call_count == 3

    @patch("apollo.interfaces.azure.auth.time.sleep")
    @patch("apollo.interfaces.azure.auth.requests.get")
    @patch.dict(
        os.environ, {"WEBSITE_HOSTNAME": "myfunc.azurewebsites.net"}, clear=True
    )
    def test_verify_retries_on_connection_error_then_succeeds(
        self, mock_get, mock_sleep
    ):
        """First attempt fails with ConnectionError, second succeeds with 401."""
        mock_get.side_effect = [ConnectionError("fail"), Mock(status_code=401)]
        result = verify_easy_auth_enforcement()
        assert result is None
        assert mock_get.call_count == 2
        mock_sleep.assert_called_once_with(1)

    @patch("apollo.interfaces.azure.auth.time.sleep")
    @patch("apollo.interfaces.azure.auth.requests.get")
    @patch.dict(
        os.environ, {"WEBSITE_HOSTNAME": "myfunc.azurewebsites.net"}, clear=True
    )
    def test_verify_returns_error_after_all_retries_exhausted(
        self, mock_get, mock_sleep
    ):
        """All 3 attempts fail with ConnectionError — returns error message."""
        mock_get.side_effect = ConnectionError("fail")
        result = verify_easy_auth_enforcement()
        assert result is not None
        assert "after 3 attempts" in result
        assert mock_get.call_count == 3
        assert mock_sleep.call_count == 2

    @patch.dict(os.environ, {}, clear=True)
    def test_verify_returns_error_when_hostname_not_set(self):
        """Without WEBSITE_HOSTNAME, verification cannot proceed."""
        result = verify_easy_auth_enforcement()
        assert result is not None
        assert "WEBSITE_HOSTNAME not set" in result

    def test_is_easy_auth_probe_with_valid_token(self):
        """A request with the correct probe token should be detected."""
        assert (
            is_easy_auth_probe({_EASY_AUTH_PROBE_HEADER: _EASY_AUTH_PROBE_TOKEN})
            is True
        )

    def test_is_easy_auth_probe_with_wrong_token(self):
        """A request with an incorrect token value should not be detected."""
        assert is_easy_auth_probe({_EASY_AUTH_PROBE_HEADER: "wrong-value"}) is False

    def test_is_easy_auth_probe_without_header(self):
        """A request without the probe header should not be detected."""
        assert is_easy_auth_probe({}) is False

    @patch("apollo.interfaces.azure.auth.requests.get")
    @patch.dict(
        os.environ, {"WEBSITE_HOSTNAME": "myfunc.azurewebsites.net"}, clear=True
    )
    def test_verify_sends_correct_url_and_headers(self, mock_get):
        """The probe should hit the correct URL with the probe header and timeout."""
        mock_get.return_value = Mock(status_code=401)
        verify_easy_auth_enforcement()
        mock_get.assert_called_once_with(
            "https://myfunc.azurewebsites.net/api/v1/test/health",
            headers={_EASY_AUTH_PROBE_HEADER: _EASY_AUTH_PROBE_TOKEN},
            timeout=10,
        )
