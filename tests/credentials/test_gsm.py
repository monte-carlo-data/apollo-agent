from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock

from box import Box

from apollo.credentials.gsm import (
    GoogleSecretManagerCredentialsService,
    SECRET_NAME,
)


class TestGcpSecretManagerCredentialsService(TestCase):
    def setUp(self):
        self.service = GoogleSecretManagerCredentialsService()

    def test_get_credentials_missing_secret_name(self):
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials({})

        self.assertEqual(
            "Missing expected secret name 'gcp_secret' in credentials",
            str(context.exception),
        )

    @patch("apollo.credentials.gsm.SecretManagerServiceClient")
    def test_get_credentials_success(self, mock_client_class: Mock):
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.access_secret_version.return_value = Box(
            payload=Box(data=b'{"username": "test", "password": "secret"}')
        )

        credentials = {
            SECRET_NAME: "test-secret",
        }

        # Execute
        result = self.service.get_credentials(credentials)

        # Verify
        mock_client.access_secret_version.assert_called_with(
            request={"name": "test-secret"}
        )
        self.assertEqual({"username": "test", "password": "secret"}, result)

    @patch("apollo.credentials.gsm.SecretManagerServiceClient")
    def test_get_credentials_gsm_error(self, mock_client_class: Mock):
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.access_secret_version.side_effect = Exception("Secret not found")

        credentials = {SECRET_NAME: "test-secret"}

        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials(credentials)

        self.assertEqual(
            "Failed to fetch credentials from GCP Secret Manager: Secret not found",
            str(context.exception),
        )

    @patch("apollo.credentials.gsm.SecretManagerServiceClient")
    def test_get_credentials_invalid_json(self, mock_client_class: Mock):
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.access_secret_version.return_value = Box(
            payload=Box(data=b"invalid json")
        )

        credentials = {SECRET_NAME: "test-secret"}

        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials(credentials)

        self.assertTrue(
            "Failed to fetch credentials from GCP Secret Manager"
            in str(context.exception)
        )

    @patch("apollo.credentials.gsm.SecretManagerServiceClient")
    def test_get_credentials_missing_secret_string(self, mock_client_class: Mock):
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.access_secret_version.return_value = Box(payload=Box(data=None))

        credentials = {SECRET_NAME: "test-secret"}

        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials(credentials)

        self.assertTrue(
            "No secret string found for secret name" in str(context.exception)
        )

    @patch("apollo.credentials.gsm.SecretManagerServiceClient")
    def test_get_credentials_merge_connect_args(self, mock_client_class: Mock):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.access_secret_version.return_value = Box(
            payload=Box(data=b'{"connect_args": {"password": "secret"}}')
        )
        credentials = {
            SECRET_NAME: "test-secret",
            "connect_args": {"username": "test"},
        }
        result = self.service.get_credentials(credentials)
        self.assertEqual(
            {"connect_args": {"username": "test", "password": "secret"}}, result
        )
