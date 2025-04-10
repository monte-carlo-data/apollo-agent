from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock

from apollo.credentials.asm import (
    AwsSecretsManagerCredentialsService,
    SECRET_NAME,
)


class TestAwsSecretsManagerCredentialsService(TestCase):
    def setUp(self):
        self.service = AwsSecretsManagerCredentialsService()

    def test_get_credentials_missing_secret_name(self):
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials({})

        self.assertEqual(
            "Missing expected secret name 'aws_secret' in credentials",
            str(context.exception),
        )

    @patch("apollo.credentials.asm.SecretsManagerProxyClient")
    def test_get_credentials_success(self, mock_client_class: Mock):
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_secret_string.return_value = (
            '{"username": "test", "password": "secret"}'
        )

        credentials = {
            SECRET_NAME: "test-secret",
            "aws_region": "us-east-1",
            "assumable_role": "arn:aws:iam::123456789012:role/test-role",
            "external_id": "test-external-id",
        }

        # Execute
        result = self.service.get_credentials(credentials)

        # Verify
        mock_client_class.assert_called_once_with(credentials=credentials)
        mock_client.get_secret_string.assert_called_once_with("test-secret")
        self.assertEqual({"username": "test", "password": "secret"}, result)

    @patch("apollo.credentials.asm.SecretsManagerProxyClient")
    def test_get_credentials_optional_params(self, mock_client_class: Mock):
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_secret_string.return_value = '{"api_key": "12345"}'

        # Only provide secret name, no optional parameters
        credentials = {SECRET_NAME: "test-secret"}

        # Execute
        result = self.service.get_credentials(credentials)

        # Verify
        mock_client_class.assert_called_once_with(credentials=credentials)
        mock_client.get_secret_string.assert_called_once_with("test-secret")
        self.assertEqual({"api_key": "12345"}, result)

    @patch("apollo.credentials.asm.SecretsManagerProxyClient")
    def test_get_credentials_asm_error(self, mock_client_class: Mock):
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_secret_string.side_effect = Exception("Secret not found")

        credentials = {SECRET_NAME: "test-secret"}

        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials(credentials)

        self.assertEqual(
            "Failed to fetch credentials from AWS Secrets Manager: Secret not found",
            str(context.exception),
        )

    @patch("apollo.credentials.asm.SecretsManagerProxyClient")
    def test_get_credentials_invalid_json(self, mock_client_class: Mock):
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_secret_string.return_value = "invalid json"

        credentials = {SECRET_NAME: "test-secret"}

        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials(credentials)

        self.assertTrue(
            "Failed to fetch credentials from AWS Secrets Manager"
            in str(context.exception)
        )

    @patch("apollo.credentials.asm.SecretsManagerProxyClient")
    def test_get_credentials_missing_secret_string(self, mock_client_class: Mock):
        # Setup
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_secret_string.return_value = None  # No SecretString in response

        credentials = {SECRET_NAME: "test-secret"}

        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials(credentials)

        self.assertTrue(
            "No secret string found for secret name" in str(context.exception)
        )

    @patch("apollo.credentials.asm.SecretsManagerProxyClient")
    def test_get_credentials_merge_connect_args(self, mock_client_class: Mock):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_secret_string.return_value = (
            '{"connect_args": {"password": "secret"}}'
        )
        credentials = {
            SECRET_NAME: "test-secret",
            "connect_args": {"username": "test"},
        }
        result = self.service.get_credentials(credentials)
        self.assertEqual(
            {"connect_args": {"username": "test", "password": "secret"}}, result
        )
