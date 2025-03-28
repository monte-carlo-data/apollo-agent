from apollo.credentials.decryption.base import BaseCredentialDecryptionService
import boto3
import base64
from botocore.client import BaseClient

KMS_KEY = "kms_key"


class KmsCredentialDecryptionService(BaseCredentialDecryptionService):
    """
    Credential decryption service that decrypts credentials using KMS.
    """

    def __init__(self, kms_client: BaseClient | None = None):
        self._kms_client = kms_client or boto3.client("kms")
        super().__init__()

    def decrypt(
        self, encrypted_credentials: str | bytes, credential_metadata: dict
    ) -> str:
        kms_key = credential_metadata.get(KMS_KEY)
        if not kms_key:
            raise ValueError("Missing expected KMS key in credentials")
        try:
            encrypted_credentials = base64.b64decode(encrypted_credentials)
        except Exception:
            raise ValueError("Failed to decode base64 encrypted credentials")
        try:
            decrypted_credentials = self._kms_client.decrypt(
                CiphertextBlob=encrypted_credentials, EncryptionContext=kms_key
            )
            return decrypted_credentials["Plaintext"].decode("utf-8")
        except Exception as e:
            raise ValueError(f"Failed to decrypt credentials using KMS: {e}")
