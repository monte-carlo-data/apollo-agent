import socket
from unittest import TestCase
from unittest.mock import patch, Mock, MagicMock

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
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

    @patch(
        "apollo.integrations.bigquery.bq_proxy_client.googleapiclient.discovery.build"
    )
    @patch(
        "apollo.integrations.bigquery.bq_proxy_client.Credentials.from_service_account_info"
    )
    def test_process_socket_default_is_not_mutated(
        self,
        mock_from_service_account,
        mock_build,
    ):
        """The timeout must not travel via the process-wide socket default.

        It used to be applied with socket.setdefaulttimeout() around build(), which
        races under the threaded workers: an overlapping build can restore another
        request's value as the process default. build_authorized_http is left
        unmocked so a reintroduced mutation would be observed here.
        """
        mock_from_service_account.return_value = MagicMock()
        mock_build.return_value = Mock()
        before = socket.getdefaulttimeout()

        # Asserting the write never happens, not just that the value is restored:
        # the old code set and restored it, so a before/after comparison alone would
        # not notice the pattern coming back.
        with patch.object(socket, "setdefaulttimeout") as mock_set_default_timeout:
            BqProxyClient(
                credentials={
                    **_SERVICE_ACCOUNT_CREDENTIALS,
                    "socket_timeout_in_seconds": 12.5,
                }
            )

        mock_set_default_timeout.assert_not_called()
        self.assertEqual(before, socket.getdefaulttimeout())

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


class BigQueryConnectionMetadataTests(TestCase):
    """`get_connection_metadata` returns the service account key's project_id so the
    DC can default the billing project when the connection omits bq_project_id."""

    _MODULE = "apollo.integrations.bigquery.bq_proxy_client"

    def _patched_client(self, credentials):
        with (
            patch(f"{self._MODULE}.build_authorized_http"),
            patch(f"{self._MODULE}.googleapiclient.discovery.build"),
            patch(f"{self._MODULE}.Credentials.from_service_account_info"),
        ):
            return BqProxyClient(credentials=credentials)

    def test_direct_credentials_return_project_id(self):
        client = self._patched_client(_SERVICE_ACCOUNT_CREDENTIALS)

        metadata = client.get_connection_metadata()

        # Exact key set: only project_id may ever be returned — the contract is
        # whitelist-by-implementation, and this is the boundary to the key material.
        self.assertEqual({"project_id"}, set(metadata.keys()))
        self.assertEqual("test-project", metadata["project_id"])

    def test_connect_args_credentials_return_project_id(self):
        """Self-hosted format: the CTP mapper puts project_id in connect_args."""
        client = self._patched_client({"connect_args": _SERVICE_ACCOUNT_CREDENTIALS})

        self.assertEqual(
            {"project_id": "test-project"}, client.get_connection_metadata()
        )

    def test_no_project_id_returns_empty_dict(self):
        """The DC's fallback depends on project_id being absent, not empty."""
        credentials = dict(_SERVICE_ACCOUNT_CREDENTIALS)
        del credentials["project_id"]
        client = self._patched_client(credentials)

        self.assertEqual({}, client.get_connection_metadata())

    def test_no_credentials_returns_empty_dict(self):
        client = self._patched_client(None)

        self.assertEqual({}, client.get_connection_metadata())

    def test_dispatches_through_execute_route(self):
        """The DC calls this as an agent operation over the generic execute endpoint."""
        agent = Agent(LoggingUtils())
        operation = {
            "trace_id": "test",
            "skip_cache": True,
            "commands": [{"method": "get_connection_metadata", "kwargs": {}}],
        }

        with (
            patch(f"{self._MODULE}.build_authorized_http"),
            patch(f"{self._MODULE}.googleapiclient.discovery.build"),
            patch(f"{self._MODULE}.Credentials.from_service_account_info"),
        ):
            response = agent.execute_operation(
                "bigquery",
                "get_connection_metadata",
                operation,
                {"connect_args": _SERVICE_ACCOUNT_CREDENTIALS},
            )

        self.assertEqual(
            {"project_id": "test-project"}, response.result["__mcd_result__"]
        )
