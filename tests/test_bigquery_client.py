from unittest import TestCase
from unittest.mock import patch, Mock, MagicMock

from apollo.integrations.bigquery.bq_proxy_client import (
    _BIGQUERY_SCOPES,
    BqProxyClient,
)


_SERVICE_ACCOUNT_CREDENTIALS = {
    "type": "service_account",
    "project_id": "test-project",
    "private_key_id": "key-id",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIE...fake...key\n-----END PRIVATE KEY-----\n",
    "client_email": "test@test-project.iam.gserviceaccount.com",
    "client_id": "123456789",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
}


class BigQueryClientTests(TestCase):
    """The client passes an explicitly configured http= rather than credentials=.

    discovery.build() rejects both together, so the assertions below check that
    credentials reach build_authorized_http instead of build().
    """

    @patch("apollo.integrations.bigquery.bq_proxy_client.build_authorized_http")
    @patch(
        "apollo.integrations.bigquery.bq_proxy_client.googleapiclient.discovery.build"
    )
    @patch(
        "apollo.integrations.bigquery.bq_proxy_client.Credentials.from_service_account_info"
    )
    def test_direct_credentials(
        self,
        mock_from_service_account,
        mock_build,
        mock_build_http,
    ):
        """Test that direct service account credentials work (legacy format)."""
        mock_credentials = MagicMock()
        mock_from_service_account.return_value = mock_credentials
        mock_http = Mock()
        mock_build_http.return_value = mock_http
        mock_build.return_value = Mock()

        client = BqProxyClient(credentials=_SERVICE_ACCOUNT_CREDENTIALS)

        self.assertIsNotNone(client)
        mock_from_service_account.assert_called_once_with(_SERVICE_ACCOUNT_CREDENTIALS)
        mock_build_http.assert_called_once_with(
            mock_credentials,
            scopes=_BIGQUERY_SCOPES,
            timeout=None,
        )
        mock_build.assert_called_once_with(
            "bigquery",
            "v2",
            cache_discovery=False,
            http=mock_http,
        )

    @patch("apollo.integrations.bigquery.bq_proxy_client.build_authorized_http")
    @patch(
        "apollo.integrations.bigquery.bq_proxy_client.googleapiclient.discovery.build"
    )
    @patch(
        "apollo.integrations.bigquery.bq_proxy_client.Credentials.from_service_account_info"
    )
    def test_direct_credentials_timeout(
        self,
        mock_from_service_account,
        mock_build,
        mock_build_http,
    ):
        """Test that socket timeout is removed from direct credentials."""
        mock_credentials = MagicMock()
        mock_from_service_account.return_value = mock_credentials
        mock_build.return_value = Mock()

        credentials_with_timeout = {
            **_SERVICE_ACCOUNT_CREDENTIALS,
            "socket_timeout_in_seconds": 12.5,
        }
        expected_credentials = dict(_SERVICE_ACCOUNT_CREDENTIALS)

        client = BqProxyClient(credentials=credentials_with_timeout)

        self.assertIsNotNone(client)
        mock_from_service_account.assert_called_once_with(expected_credentials)
        mock_build_http.assert_called_once_with(
            mock_credentials,
            scopes=_BIGQUERY_SCOPES,
            timeout=12.5,
        )

    @patch("apollo.integrations.bigquery.bq_proxy_client.build_authorized_http")
    @patch(
        "apollo.integrations.bigquery.bq_proxy_client.googleapiclient.discovery.build"
    )
    @patch(
        "apollo.integrations.bigquery.bq_proxy_client.Credentials.from_service_account_info"
    )
    def test_connect_args_credentials(
        self,
        mock_from_service_account,
        mock_build,
        mock_build_http,
    ):
        """Test that credentials wrapped in connect_args work (self-hosted format)."""
        mock_credentials = MagicMock()
        mock_from_service_account.return_value = mock_credentials
        mock_http = Mock()
        mock_build_http.return_value = mock_http
        mock_build.return_value = Mock()

        credentials_with_connect_args = {
            "connect_args": _SERVICE_ACCOUNT_CREDENTIALS,
        }

        client = BqProxyClient(credentials=credentials_with_connect_args)

        self.assertIsNotNone(client)
        # Should extract credentials from connect_args
        mock_from_service_account.assert_called_once_with(_SERVICE_ACCOUNT_CREDENTIALS)
        mock_build_http.assert_called_once_with(
            mock_credentials,
            scopes=_BIGQUERY_SCOPES,
            timeout=None,
        )
        mock_build.assert_called_once_with(
            "bigquery",
            "v2",
            cache_discovery=False,
            http=mock_http,
        )

    @patch("apollo.integrations.bigquery.bq_proxy_client.build_authorized_http")
    @patch(
        "apollo.integrations.bigquery.bq_proxy_client.googleapiclient.discovery.build"
    )
    @patch(
        "apollo.integrations.bigquery.bq_proxy_client.Credentials.from_service_account_info"
    )
    def test_connect_args_timeout(
        self,
        mock_from_service_account,
        mock_build,
        mock_build_http,
    ):
        """Test that socket timeout uses a custom HTTP client."""
        mock_credentials = MagicMock()
        mock_from_service_account.return_value = mock_credentials
        mock_build.return_value = Mock()

        credentials_with_timeout = {
            "connect_args": {
                **_SERVICE_ACCOUNT_CREDENTIALS,
                "socket_timeout_in_seconds": 12.5,
            },
        }

        client = BqProxyClient(credentials=credentials_with_timeout)

        self.assertIsNotNone(client)
        mock_from_service_account.assert_called_once_with(_SERVICE_ACCOUNT_CREDENTIALS)
        mock_build_http.assert_called_once_with(
            mock_credentials,
            scopes=_BIGQUERY_SCOPES,
            timeout=12.5,
        )

    @patch("apollo.integrations.bigquery.bq_proxy_client.build_authorized_http")
    @patch(
        "apollo.integrations.bigquery.bq_proxy_client.googleapiclient.discovery.build"
    )
    @patch(
        "apollo.integrations.bigquery.bq_proxy_client.Credentials.from_service_account_info"
    )
    def test_no_credentials_uses_adc(
        self,
        mock_from_service_account,
        mock_build,
        mock_build_http,
    ):
        """Test that when no credentials are provided, ADC is used."""
        mock_http = Mock()
        mock_build_http.return_value = mock_http
        mock_build.return_value = Mock()

        client = BqProxyClient(credentials=None)

        self.assertIsNotNone(client)
        # Should not call from_service_account_info when no credentials
        mock_from_service_account.assert_not_called()
        # None credentials means build_authorized_http resolves ADC
        mock_build_http.assert_called_once_with(
            None,
            scopes=_BIGQUERY_SCOPES,
            timeout=None,
        )
        mock_build.assert_called_once_with(
            "bigquery",
            "v2",
            cache_discovery=False,
            http=mock_http,
        )

    @patch("apollo.integrations.bigquery.bq_proxy_client.build_authorized_http")
    @patch(
        "apollo.integrations.bigquery.bq_proxy_client.googleapiclient.discovery.build"
    )
    @patch(
        "apollo.integrations.bigquery.bq_proxy_client.Credentials.from_service_account_info"
    )
    def test_empty_credentials_uses_adc(
        self,
        mock_from_service_account,
        mock_build,
        mock_build_http,
    ):
        """Test that when empty credentials dict is provided, ADC is used."""
        mock_build.return_value = Mock()

        client = BqProxyClient(credentials={})

        self.assertIsNotNone(client)
        # Empty dict is falsy, so should not call from_service_account_info
        mock_from_service_account.assert_not_called()
        mock_build_http.assert_called_once_with(
            None,
            scopes=_BIGQUERY_SCOPES,
            timeout=None,
        )
