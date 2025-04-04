from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock
import base64

from apollo.credentials.decryption.kms import KmsCredentialDecryptionService

KMS_KEY_ID = "test-key-id"
KMS_KEY_ID_FIELD = "kms_key_id"
CREDS_WITH_KMS = {KMS_KEY_ID_FIELD: KMS_KEY_ID}


class TestKmsCredentialDecryptionService(TestCase):
    def setUp(self):
        self.service = KmsCredentialDecryptionService()

    @patch("apollo.credentials.decryption.kms.KmsProxyClient")
    def test_decrypt_success(self, mock_kms_class: Mock):
        # Setup
        mock_kms_instance = MagicMock()
        mock_kms_class.return_value = mock_kms_instance
        mock_kms_instance.decrypt.return_value = "decrypted_value"

        # Base64 encoded string as it would come from env var
        encrypted_credentials = base64.b64encode(b"encrypted_data").decode("utf-8")

        # Execute
        result = self.service.decrypt(encrypted_credentials, CREDS_WITH_KMS)

        # Verify
        mock_kms_class.assert_called_once_with(credentials=CREDS_WITH_KMS)
        mock_kms_instance.decrypt.assert_called_once_with(
            encrypted_credentials, KMS_KEY_ID
        )
        self.assertEqual("decrypted_value", result)

    def test_decrypt_missing_KMS_KEY_ID(self):
        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.decrypt("test", {})

        self.assertEqual(
            "Missing expected KMS key in credentials", str(context.exception)
        )

    @patch("apollo.credentials.decryption.kms.KmsProxyClient")
    def test_decrypt_kms_error(self, mock_kms_class: Mock):
        # Setup
        mock_kms_instance = MagicMock()
        mock_kms_class.return_value = mock_kms_instance
        mock_kms_instance.decrypt.side_effect = ValueError(
            "Failed to decrypt credentials using KMS: An error occurred (Unknown) when calling the Decrypt operation: KMS error"
        )

        encrypted_credentials = base64.b64encode(b"encrypted_data").decode("utf-8")

        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.decrypt(encrypted_credentials, CREDS_WITH_KMS)

        self.assertEqual(
            "Failed to decrypt credentials using KMS: An error occurred (Unknown) when calling the Decrypt operation: KMS error",
            str(context.exception),
        )

    @patch("apollo.credentials.decryption.kms.KmsProxyClient")
    def test_decrypt_returns_string(self, mock_kms_class: Mock):
        # Setup
        mock_kms_instance = MagicMock()
        mock_kms_class.return_value = mock_kms_instance
        mock_kms_instance.decrypt.return_value = '{"key": "value"}'

        encrypted_credentials = base64.b64encode(b"encrypted_data").decode("utf-8")

        # Execute
        result = self.service.decrypt(encrypted_credentials, CREDS_WITH_KMS)

        # Verify
        mock_kms_class.assert_called_once_with(credentials=CREDS_WITH_KMS)
        mock_kms_instance.decrypt.assert_called_once_with(
            encrypted_credentials, KMS_KEY_ID
        )
        self.assertIsInstance(result, str)
        self.assertEqual('{"key": "value"}', result)
