import os
import tempfile
from unittest import TestCase
from unittest.mock import patch, mock_open

from apollo.agent.agent import Agent
from apollo.agent.env_vars import MCD_AWS_CA_BUNDLE_SECRET_NAME_ENV_VAR
from apollo.agent.logging_utils import LoggingUtils


class TestAwsCaBundleSetup(TestCase):
    """Test cases for AWS CA Bundle setup functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Sample CA certificate data for testing
        self.test_ca_data = """-----BEGIN CERTIFICATE-----
MIIDdzCCAl+gAwIBAgIEAgAAuTANBgkqhkiG9w0BAQUFADBaMQswCQYDVQQGEwJJ
RTESMBAGA1UEChMJQmFsdGltb3JlMRMwEQYDVQQLEwpDeWJlclRydXN0MSIwIAYD
VQQDExlCYWx0aW1vcmUgQ3liZXJUcnVzdCBSb290MB4XDTAwMDUxMjE4NDYwMFoX
DTI1MDUxMjIzNTkwMFowWjELMAkGA1UEBhMCSUUxEjAQBgNVBAoTCUJhbHRpbW9y
ZTETMBEGA1UECxMKQ3liZXJUcnVzdDEiMCAGA1UEAxMZQmFsdGltb3JlIEN5YmVy
VHJ1c3QgUm9vdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKMEuyKr
mD1X6CZymrV51Cni4eiVgLGw41uOKymaZN+hXe2wCQVt2yguzmKiYv60iNoS6zjr
IZ3AQSsBUnuId9Mcj8e6uYi1agnnc+gRQKfRzMpijS3ljwumUNKoUMMo6vWrJYeK
mpYcqWe4PwzV9/lSEy/CG9VwcPCPwBLKBsua4dnKM3p31vjsufFoREJIE9LAwqSu
XmD+tqYF/LTdB1kC1FkYmGP1pWPgkAx9XbIGevOF6uvUA65ehD5f/xXtabz5OTZy
dc93Uk3zyZAsuT3lySNTPx8kmCFcB5kpvcY67Oduhjprl3RjM71oGDHweI12v/ye
jl0qhqdNkNwnGjkCAwEAAaNFMEMwHQYDVR0OBBYEFOWdWTCCR1jMrPoIVDaGezq1
BE3wMBIGA1UdEwEB/wQIMAYBAf8CAQMwDgYDVR0PAQH/BAQDAgEGMA0GCSqGSIb3
DQEBBQUAA4IBAQCFDF2O5G9RaEIFoN27TyclhAO992T9Ldcw46QQF+vaKSm2eT92
9hkTI7gQCvlYpNRhcL0EYWoSihfVCr3FvDB81ukMJY2GQE/szKN+OMY3EU/t3Wgx
jkzSswF07r51XgdIGn9w/xZchMB5hbgF/X++ZRGjD8ACtPhSNzkE1akxehi/oCr0
Epn3o0WC4zxe9Z2etciefC7IpJ5OCBRLbf1wbWsaY71k5h+3zvDyny67G7fyUIhz
ksLi4xaNmjICq44Y3ekQEe5+NauQrz4wlHrQMz2nZQ/1/I6eYs9HRCwBXbsdtTLS
R9I4LtD+gdwyah617jzV/OeBHRnDJELqYzmp
-----END CERTIFICATE-----"""

    def tearDown(self):
        """Clean up after each test."""
        # Clean up environment variables
        env_vars_to_clean = [MCD_AWS_CA_BUNDLE_SECRET_NAME_ENV_VAR, "AWS_CA_BUNDLE"]
        for env_var in env_vars_to_clean:
            if env_var in os.environ:
                del os.environ[env_var]

    @patch.dict(os.environ, {}, clear=True)
    def test_setup_aws_ca_bundle_no_env_var(self):
        """Test that setup_aws_ca_bundle does nothing when MCD_AWS_CA_BUNDLE_SECRET_NAME is not set."""
        # Ensure the environment variable is not set
        self.assertNotIn(MCD_AWS_CA_BUNDLE_SECRET_NAME_ENV_VAR, os.environ)

        # Call the method by creating an Agent
        Agent(LoggingUtils())

        # AWS_CA_BUNDLE should not be set
        self.assertNotIn("AWS_CA_BUNDLE", os.environ)

    @patch.dict(os.environ, {MCD_AWS_CA_BUNDLE_SECRET_NAME_ENV_VAR: ""})
    def test_setup_aws_ca_bundle_empty_env_var(self):
        """Test that setup_aws_ca_bundle does nothing when MCD_AWS_CA_BUNDLE_SECRET_NAME is empty."""
        # Call the method by creating an Agent
        Agent(LoggingUtils())

        # AWS_CA_BUNDLE should not be set
        self.assertNotIn("AWS_CA_BUNDLE", os.environ)

    @patch("os.makedirs")
    @patch("os.chmod")
    @patch("builtins.open", new_callable=mock_open)
    @patch("apollo.agent.utils.AgentUtils.ensure_temp_path")
    @patch("apollo.agent.agent.SecretsManagerProxyClient")
    def test_setup_aws_ca_bundle_creates_file(
        self,
        mock_asm_client_class,
        mock_ensure_temp_path,
        mock_file_open,
        mock_chmod,
        mock_makedirs,
    ):
        """Test that setup_aws_ca_bundle creates a file when fetching from AWS Secrets Manager."""
        # Set up mocks
        temp_path = "/tmp/ca_bundle"
        ca_bundle_path = "/tmp/ca_bundle/aws_ca_bundle.pem"
        secret_name = "my-ca-bundle-secret"

        mock_ensure_temp_path.return_value = temp_path

        # Mock the ASM client
        mock_asm_client = mock_asm_client_class.return_value
        mock_asm_client.get_secret_string.return_value = self.test_ca_data

        # Set environment variable and test inside the context
        with patch.dict(
            os.environ, {MCD_AWS_CA_BUNDLE_SECRET_NAME_ENV_VAR: secret_name}
        ):
            Agent(LoggingUtils())

            # Verify calls
            mock_ensure_temp_path.assert_called_once_with("ca_bundle")
            mock_asm_client_class.assert_called_once_with(credentials=None)
            mock_asm_client.get_secret_string.assert_called_once_with(secret_name)
            mock_file_open.assert_called_once_with(ca_bundle_path, "w")
            mock_file_open().write.assert_called_once_with(self.test_ca_data)
            mock_chmod.assert_called_once_with(ca_bundle_path, 0o600)

            # Verify AWS_CA_BUNDLE is set
            self.assertEqual(os.environ.get("AWS_CA_BUNDLE"), ca_bundle_path)

    @patch("os.path.exists")
    @patch("os.chmod")
    @patch("builtins.open", new_callable=mock_open)
    @patch("apollo.agent.utils.AgentUtils.ensure_temp_path")
    @patch("apollo.agent.agent.SecretsManagerProxyClient")
    def test_setup_aws_ca_bundle_file_creation_error(
        self,
        mock_asm_client_class,
        mock_ensure_temp_path,
        mock_file_open,
        mock_chmod,
        mock_path_exists,
    ):
        """Test that setup_aws_ca_bundle raises ValueError when file creation fails."""
        # Set up mocks
        temp_path = "/tmp/ca_bundle"
        ca_bundle_path = "/tmp/ca_bundle/aws_ca_bundle.pem"
        secret_name = "my-ca-bundle-secret"

        mock_ensure_temp_path.return_value = temp_path
        mock_path_exists.return_value = False
        mock_file_open.side_effect = IOError("Permission denied")

        # Mock the ASM client
        mock_asm_client = mock_asm_client_class.return_value
        mock_asm_client.get_secret_string.return_value = self.test_ca_data

        # Set environment variable and expect ValueError
        with patch.dict(
            os.environ, {MCD_AWS_CA_BUNDLE_SECRET_NAME_ENV_VAR: secret_name}
        ):
            with self.assertRaises(ValueError) as context:
                Agent(LoggingUtils())

            self.assertIn(
                "Failed to setup CA bundle from secret", str(context.exception)
            )
            self.assertIn(secret_name, str(context.exception))
            self.assertIn("Permission denied", str(context.exception))

    @patch("os.path.exists")
    @patch("apollo.agent.utils.AgentUtils.ensure_temp_path")
    @patch("apollo.agent.agent.SecretsManagerProxyClient")
    def test_setup_aws_ca_bundle_secret_not_found(
        self, mock_asm_client_class, mock_ensure_temp_path, mock_path_exists
    ):
        """Test that setup_aws_ca_bundle raises ValueError when secret is not found."""
        # Set up mocks
        temp_path = "/tmp/ca_bundle"
        ca_bundle_path = "/tmp/ca_bundle/aws_ca_bundle.pem"
        secret_name = "non-existent-secret"

        mock_ensure_temp_path.return_value = temp_path
        mock_path_exists.return_value = False

        # Mock the ASM client to return None (secret not found)
        mock_asm_client = mock_asm_client_class.return_value
        mock_asm_client.get_secret_string.return_value = None

        # Set environment variable and expect ValueError
        with patch.dict(
            os.environ, {MCD_AWS_CA_BUNDLE_SECRET_NAME_ENV_VAR: secret_name}
        ):
            with self.assertRaises(ValueError) as context:
                Agent(LoggingUtils())

            self.assertIn("No secret string found", str(context.exception))
            self.assertIn(secret_name, str(context.exception))

    @patch("os.path.exists")
    @patch("apollo.agent.utils.AgentUtils.ensure_temp_path")
    @patch("apollo.agent.agent.SecretsManagerProxyClient")
    def test_setup_aws_ca_bundle_asm_error(
        self, mock_asm_client_class, mock_ensure_temp_path, mock_path_exists
    ):
        """Test that setup_aws_ca_bundle raises ValueError when ASM client fails."""
        # Set up mocks
        temp_path = "/tmp/ca_bundle"
        ca_bundle_path = "/tmp/ca_bundle/aws_ca_bundle.pem"
        secret_name = "my-ca-bundle-secret"

        mock_ensure_temp_path.return_value = temp_path
        mock_path_exists.return_value = False

        # Mock the ASM client to raise an exception
        mock_asm_client_class.side_effect = Exception("AWS credentials not found")

        # Set environment variable and expect ValueError
        with patch.dict(
            os.environ, {MCD_AWS_CA_BUNDLE_SECRET_NAME_ENV_VAR: secret_name}
        ):
            with self.assertRaises(ValueError) as context:
                Agent(LoggingUtils())

            self.assertIn(
                "Failed to setup CA bundle from secret", str(context.exception)
            )
            self.assertIn(secret_name, str(context.exception))
            self.assertIn("AWS credentials not found", str(context.exception))

    @patch("apollo.agent.agent.SecretsManagerProxyClient")
    def test_integration_with_real_temp_directory(self, mock_asm_client_class):
        """Integration test using real temporary directory."""
        secret_name = "my-ca-bundle-secret"

        # Mock the ASM client
        mock_asm_client = mock_asm_client_class.return_value
        mock_asm_client.get_secret_string.return_value = self.test_ca_data

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch(
                "apollo.agent.utils.AgentUtils.temp_path", return_value=temp_dir
            ):
                # Set environment variable
                with patch.dict(
                    os.environ, {MCD_AWS_CA_BUNDLE_SECRET_NAME_ENV_VAR: secret_name}
                ):
                    Agent(LoggingUtils())

                    # Verify AWS_CA_BUNDLE is set
                    aws_ca_bundle = os.environ.get("AWS_CA_BUNDLE")
                    self.assertIsNotNone(aws_ca_bundle)

                    # Verify file exists and contains expected content
                    self.assertTrue(os.path.exists(aws_ca_bundle))

                    with open(aws_ca_bundle, "r") as f:
                        file_content = f.read()

                    self.assertEqual(file_content, self.test_ca_data)

                    # Verify file permissions are correct (600 = rw-------)
                    file_stat = os.stat(aws_ca_bundle)
                    file_permissions = oct(file_stat.st_mode)[-3:]
                    self.assertEqual(file_permissions, "600")

                    # Verify ASM client was called correctly
                    mock_asm_client_class.assert_called_once_with(credentials=None)
                    mock_asm_client.get_secret_string.assert_called_once_with(
                        secret_name
                    )
