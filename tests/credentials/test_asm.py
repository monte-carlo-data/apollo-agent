from unittest import TestCase
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

from apollo.credentials.asm import (
    AwsSecretsManagerCredentialsService,
    SECRET_NAME,
    REGION,
    ROLE_ARN,
    EXTERNAL_ID,
)


class TestAwsSecretsManagerCredentialsService(TestCase):
    def setUp(self):
        self.service = AwsSecretsManagerCredentialsService()

    def test_get_credentials_missing_secret_name(self):
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials({})

        self.assertEqual(
            "Missing expected secret name in credentials", str(context.exception)
        )

    @patch("boto3.client")
    def test_get_credentials_success(self, mock_boto3_client: Mock):
        # Setup
        mock_asm = Mock()
        mock_boto3_client.return_value = mock_asm
        mock_asm.get_secret_value.return_value = {
            "SecretString": '{"username": "test", "password": "secret"}'
        }

        credentials = {
            SECRET_NAME: "test-secret",
            REGION: "us-east-1",
            ROLE_ARN: "test-role",
            EXTERNAL_ID: "test-external-id",
        }

        # Execute
        result = self.service.get_credentials(credentials)

        # Verify
        mock_boto3_client.assert_called_once_with(
            "secretsmanager",
            region_name="us-east-1",
            aws_access_key_id="test-role",
            aws_secret_access_key="test-external-id",
        )
        mock_asm.get_secret_value.assert_called_once_with(SecretId="test-secret")
        self.assertEqual({"username": "test", "password": "secret"}, result)

    @patch("boto3.client")
    def test_get_credentials_optional_params(self, mock_boto3_client: Mock):
        # Setup
        mock_asm = Mock()
        mock_boto3_client.return_value = mock_asm
        mock_asm.get_secret_value.return_value = {
            "SecretString": '{"api_key": "12345"}'
        }

        # Only provide secret name, no optional parameters
        credentials = {SECRET_NAME: "test-secret"}

        # Execute
        result = self.service.get_credentials(credentials)

        # Verify
        mock_boto3_client.assert_called_once_with(
            "secretsmanager",
            region_name=None,
            aws_access_key_id=None,
            aws_secret_access_key=None,
        )
        mock_asm.get_secret_value.assert_called_once_with(SecretId="test-secret")
        self.assertEqual({"api_key": "12345"}, result)

    @patch("boto3.client")
    def test_get_credentials_asm_error(self, mock_boto3_client: Mock):
        # Setup
        mock_asm = Mock()
        mock_boto3_client.return_value = mock_asm
        mock_asm.get_secret_value.side_effect = ClientError(
            error_response={"Error": {"Message": "Secret not found"}},
            operation_name="GetSecretValue",
        )

        credentials = {SECRET_NAME: "test-secret"}

        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials(credentials)

        self.assertEqual(
            "Failed to fetch credentials from AWS Secrets Manager: An error occurred (Unknown) when calling the GetSecretValue operation: Secret not found",
            str(context.exception),
        )

    @patch("boto3.client")
    def test_get_credentials_invalid_json(self, mock_boto3_client: Mock):
        # Setup
        mock_asm = Mock()
        mock_boto3_client.return_value = mock_asm
        mock_asm.get_secret_value.return_value = {"SecretString": "invalid json"}

        credentials = {SECRET_NAME: "test-secret"}

        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials(credentials)

        self.assertTrue(
            "Failed to fetch credentials from AWS Secrets Manager"
            in str(context.exception)
        )

    @patch("boto3.client")
    def test_get_credentials_missing_secret_string(self, mock_boto3_client: Mock):
        # Setup
        mock_asm = Mock()
        mock_boto3_client.return_value = mock_asm
        mock_asm.get_secret_value.return_value = {}  # No SecretString in response

        credentials = {SECRET_NAME: "test-secret"}

        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.get_credentials(credentials)

        self.assertTrue(
            "Failed to fetch credentials from AWS Secrets Manager"
            in str(context.exception)
        )
