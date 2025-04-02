from unittest import TestCase
from unittest.mock import Mock, patch
import base64

from apollo.credentials.decryption.kms import KmsCredentialDecryptionService, KMS_KEY


class TestKmsCredentialDecryptionService(TestCase):
    def setUp(self):
        self.service = KmsCredentialDecryptionService()

    @patch("apollo.integrations.aws.kms_proxy_client.KmsProxyClient.decrypt")
    def test_decrypt_success(self, mock_decrypt: Mock):
        # Setup
        mock_decrypt.return_value = "decrypted_value"

        # Base64 encoded string as it would come from env var
        encrypted_credentials = base64.b64encode(b"encrypted_data").decode("utf-8")
        kms_key = {"KeyId": "test-key-id"}

        # Execute
        result = self.service.decrypt(encrypted_credentials, {KMS_KEY: kms_key})

        # Verify
        mock_decrypt.assert_called_once_with(encrypted_credentials, kms_key)
        self.assertEqual("decrypted_value", result)

    def test_decrypt_missing_kms_key(self):
        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.decrypt("test", {})

        self.assertEqual(
            "Missing expected KMS key in credentials", str(context.exception)
        )

    @patch("apollo.integrations.aws.kms_proxy_client.KmsProxyClient.decrypt")
    def test_decrypt_kms_error(self, mock_decrypt: Mock):
        # Setup
        mock_decrypt.side_effect = ValueError(
            "Failed to decrypt credentials using KMS: An error occurred (Unknown) when calling the Decrypt operation: KMS error"
        )

        encrypted_credentials = base64.b64encode(b"encrypted_data").decode("utf-8")
        kms_key = {"KeyId": "test-key-id"}

        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.decrypt(encrypted_credentials, {KMS_KEY: kms_key})

        self.assertEqual(
            "Failed to decrypt credentials using KMS: An error occurred (Unknown) when calling the Decrypt operation: KMS error",
            str(context.exception),
        )

    @patch("apollo.integrations.aws.kms_proxy_client.KmsProxyClient.decrypt")
    def test_decrypt_returns_string(self, mock_decrypt: Mock):
        # Setup
        mock_decrypt.return_value = '{"key": "value"}'

        encrypted_credentials = base64.b64encode(b"encrypted_data").decode("utf-8")
        kms_key = {"KeyId": "test-key-id"}

        # Execute
        result = self.service.decrypt(encrypted_credentials, {KMS_KEY: kms_key})

        # Verify
        self.assertIsInstance(result, str)
        self.assertEqual('{"key": "value"}', result)
