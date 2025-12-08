from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock

from box import Box

from apollo.credentials.akv import (
    AzureKeyVaultCredentialsService,
    VAULT_NAME,
    VAULT_URL,
)
from apollo.credentials.akv import SECRET_NAME


class TestAzureKeyVaultCredentialsService(TestCase):
    def setUp(self):
        self.service = AzureKeyVaultCredentialsService()

    def test_get_credentials_missing_secret_name(self):
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials({})

        self.assertEqual(
            "Missing expected secret name 'akv_secret' in credentials",
            str(context.exception),
        )

    def test_get_credentials_missing_url_name(self):
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials({SECRET_NAME: "test-secret"})

        self.assertEqual(
            "One of 'akv_vault_url' or 'akv_vault_name' is required in credentials",
            str(context.exception),
        )

    @patch("apollo.credentials.akv.SecretClient")
    def test_get_credentials_success(self, mock_client_class: Mock):
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_secret.return_value = Box(
            value='{"username": "test", "password": "secret"}'
        )

        credentials = {
            SECRET_NAME: "test-secret",
            VAULT_NAME: "test-vault",
        }

        # Execute
        result = self.service.get_credentials(credentials)

        # Verify
        mock_client.get_secret.assert_called_with("test-secret")
        self.assertEqual({"username": "test", "password": "secret"}, result)

    @patch("apollo.credentials.akv.SecretClient")
    def test_get_credentials_akv_error(self, mock_client_class: Mock):
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_secret.side_effect = Exception("Secret not found")

        credentials = {
            SECRET_NAME: "test-secret",
            VAULT_URL: "https://test-vault.vault.azure.net/",
        }

        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials(credentials)

        self.assertEqual(
            "Failed to fetch credentials from Azure Key Vault: Secret not found",
            str(context.exception),
        )

    @patch("apollo.credentials.akv.SecretClient")
    def test_get_credentials_invalid_json(self, mock_client_class: Mock):
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_secret.return_value = Box(value="invalid json")

        credentials = {SECRET_NAME: "test-secret", VAULT_NAME: "test-vault"}

        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials(credentials)

        self.assertIn(
            "Failed to fetch credentials from Azure Key Vault", str(context.exception)
        )

    @patch("apollo.credentials.akv.SecretClient")
    def test_get_credentials_missing_secret_string(self, mock_client_class: Mock):
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_secret.return_value = Box(value=None)

        credentials = {SECRET_NAME: "test-secret", VAULT_NAME: "test-vault"}

        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials(credentials)

        self.assertTrue(
            "No secret string found for secret name" in str(context.exception)
        )

    @patch("apollo.credentials.akv.SecretClient")
    def test_get_credentials_merge_connect_args(self, mock_client_class: Mock):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_secret.return_value = Box(
            value='{"connect_args": {"password": "secret"}}'
        )
        credentials = {
            SECRET_NAME: "test-secret",
            VAULT_NAME: "test-vault",
            "connect_args": {"username": "test"},
        }
        result = self.service.get_credentials(credentials)
        self.assertEqual(
            {"connect_args": {"username": "test", "password": "secret"}}, result
        )
