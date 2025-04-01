from unittest import TestCase
from unittest.mock import Mock
import base64

from botocore.exceptions import ClientError

from apollo.credentials.decryption.kms import KmsCredentialDecryptionService, KMS_KEY


class TestKmsCredentialDecryptionService(TestCase):
    def setUp(self):
        self.mock_kms = Mock()
        self.service = KmsCredentialDecryptionService(self.mock_kms)

    def test_decrypt_success(self):
        # Setup
        self.mock_kms.decrypt.return_value = {"Plaintext": b"decrypted_value"}

        # Base64 encoded string as it would come from env var
        encrypted_credentials = base64.b64encode(b"encrypted_data").decode("utf-8")
        kms_key = {"KeyId": "test-key-id"}

        # Execute
        result = self.service.decrypt(encrypted_credentials, {KMS_KEY: kms_key})

        # Verify
        self.mock_kms.decrypt.assert_called_once_with(
            CiphertextBlob=b"encrypted_data",  # Should be decoded from base64
            EncryptionContext=kms_key,
        )
        self.assertEqual("decrypted_value", result)  # Should be decoded to string

    def test_decrypt_missing_kms_key(self):
        # Execute & Verify
        with self.assertRaises(ValueError) as context:
            self.service.decrypt("test", {})

        self.assertEqual(
            "Missing expected KMS key in credentials", str(context.exception)
        )

    def test_decrypt_kms_error(self):
        # Setup
        self.mock_kms.decrypt.side_effect = ClientError(
            error_response={"Error": {"Message": "KMS error"}}, operation_name="Decrypt"
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

    def test_decrypt_returns_string(self):
        # Setup
        self.mock_kms.decrypt.return_value = {"Plaintext": b'{"key": "value"}'}

        encrypted_credentials = base64.b64encode(b"encrypted_data").decode("utf-8")
        kms_key = {"KeyId": "test-key-id"}

        # Execute
        result = self.service.decrypt(encrypted_credentials, {KMS_KEY: kms_key})

        # Verify
        self.assertIsInstance(result, str)
        self.assertEqual('{"key": "value"}', result)
