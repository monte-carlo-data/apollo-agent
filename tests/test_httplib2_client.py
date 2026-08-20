import os
import socket
from unittest import TestCase
from unittest.mock import MagicMock, patch

import httplib2
from google_auth_httplib2 import AuthorizedHttp

from apollo.integrations.http.httplib2_client import (
    DEFAULT_DECODE_LIMIT_RATIO,
    DEFAULT_TIMEOUT_SECONDS,
    build_authorized_http,
    resolve_decode_limit_ratio,
)

_SCOPES = ["https://www.googleapis.com/auth/bigquery"]

# Both spellings must be cleared in every test: httplib2 itself reads the lowercase
# name first and the uppercase one second, and we mirror that precedence.
_ENV_VARS = ("httplib2_decode_limit_ratio", "HTTPLIB2_DECODE_LIMIT_RATIO")


class ResolveDecodeLimitRatioTests(TestCase):
    def setUp(self):
        for name in _ENV_VARS:
            os.environ.pop(name, None)

    def tearDown(self):
        for name in _ENV_VARS:
            os.environ.pop(name, None)

    def test_default_is_above_observed_bigquery_ratios(self):
        # httplib2's own default is 100, which legitimate BigQuery responses exceed.
        self.assertEqual(500.0, resolve_decode_limit_ratio())
        self.assertEqual(500.0, DEFAULT_DECODE_LIMIT_RATIO)

    def test_lowercase_env_var_overrides_default(self):
        os.environ["httplib2_decode_limit_ratio"] = "250"
        self.assertEqual(250.0, resolve_decode_limit_ratio())

    def test_uppercase_env_var_overrides_default(self):
        os.environ["HTTPLIB2_DECODE_LIMIT_RATIO"] = "250"
        self.assertEqual(250.0, resolve_decode_limit_ratio())

    def test_lowercase_env_var_wins_over_uppercase(self):
        # Matches httplib2's own precedence, so operators get the same result whether
        # the limit is applied here or by httplib2 directly.
        os.environ["httplib2_decode_limit_ratio"] = "111"
        os.environ["HTTPLIB2_DECODE_LIMIT_RATIO"] = "222"
        self.assertEqual(111.0, resolve_decode_limit_ratio())

    def test_invalid_env_var_falls_back_to_default(self):
        # httplib2 would silently fall back to 100 here, re-breaking collection.
        os.environ["HTTPLIB2_DECODE_LIMIT_RATIO"] = "not-a-number"
        self.assertEqual(500.0, resolve_decode_limit_ratio())


class BuildAuthorizedHttpTests(TestCase):
    def setUp(self):
        for name in _ENV_VARS:
            os.environ.pop(name, None)

    def test_decode_limit_ratio_is_applied_to_the_http_client(self):
        http = build_authorized_http(MagicMock(), scopes=_SCOPES).http

        self.assertEqual({"ratio": 500.0}, http.limit_kwargs)

    def test_decode_limit_ratio_honors_env_override(self):
        os.environ["HTTPLIB2_DECODE_LIMIT_RATIO"] = "750"
        try:
            http = build_authorized_http(MagicMock(), scopes=_SCOPES).http
        finally:
            os.environ.pop("HTTPLIB2_DECODE_LIMIT_RATIO", None)

        self.assertEqual({"ratio": 750.0}, http.limit_kwargs)

    def test_explicit_timeout_is_applied(self):
        authorized_http = build_authorized_http(
            MagicMock(), scopes=_SCOPES, timeout=12.5
        )

        self.assertEqual(12.5, authorized_http.http.timeout)

    def test_timeout_falls_back_to_socket_default(self):
        # Same rule as googleapiclient's build_http(), but without mutating the global.
        with patch.object(socket, "getdefaulttimeout", return_value=30.0):
            authorized_http = build_authorized_http(MagicMock(), scopes=_SCOPES)

        self.assertEqual(30.0, authorized_http.http.timeout)

    def test_timeout_falls_back_to_default_when_no_socket_default(self):
        with patch.object(socket, "getdefaulttimeout", return_value=None):
            authorized_http = build_authorized_http(MagicMock(), scopes=_SCOPES)

        self.assertEqual(DEFAULT_TIMEOUT_SECONDS, authorized_http.http.timeout)
        self.assertEqual(60, DEFAULT_TIMEOUT_SECONDS)

    def test_308_is_not_treated_as_a_redirect(self):
        # build_http() does the same: Google APIs use 308 for resumable uploads.
        http = build_authorized_http(MagicMock(), scopes=_SCOPES).http

        self.assertNotIn(308, http.redirect_codes)

    def test_returns_authorized_http_wrapping_the_configured_client(self):
        authorized_http = build_authorized_http(MagicMock(), scopes=_SCOPES)

        self.assertIsInstance(authorized_http, AuthorizedHttp)
        self.assertIsInstance(authorized_http.http, httplib2.Http)

    @patch("apollo.integrations.http.httplib2_client.with_scopes_if_required")
    def test_credentials_are_scoped(self, mock_with_scopes):
        # discovery.build() scopes credentials itself, but only on the path that takes
        # credentials= — passing http= bypasses it, so we must scope them here or
        # service-account credentials fail to refresh.
        credentials = MagicMock()
        scoped = MagicMock()
        mock_with_scopes.return_value = scoped

        authorized_http = build_authorized_http(credentials, scopes=_SCOPES)

        mock_with_scopes.assert_called_once_with(credentials, _SCOPES)
        self.assertIs(scoped, authorized_http.credentials)

    @patch("apollo.integrations.http.httplib2_client.google.auth.default")
    def test_no_credentials_falls_back_to_adc(self, mock_default):
        adc_credentials = MagicMock()
        mock_default.return_value = (adc_credentials, "test-project")

        authorized_http = build_authorized_http(None, scopes=_SCOPES)

        mock_default.assert_called_once_with(scopes=_SCOPES)
        self.assertIs(adc_credentials, authorized_http.credentials)
