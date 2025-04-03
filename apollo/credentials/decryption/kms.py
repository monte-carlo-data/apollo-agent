from apollo.credentials.decryption.base import BaseCredentialDecryptionService
import base64

from apollo.integrations.aws.kms_proxy_client import KmsProxyClient

KMS_KEY_ID = "kms_key_id"


class KmsCredentialDecryptionService(BaseCredentialDecryptionService):
    """
    Credential decryption service that decrypts credentials using KMS.
    """

    def decrypt(
        self, encrypted_credentials: str | bytes, credential_metadata: dict
    ) -> str:
        kms_key_id = credential_metadata.get(KMS_KEY_ID)
        if not kms_key_id:
            raise ValueError("Missing expected KMS key in credentials")

        kms_client = KmsProxyClient(credentials=credential_metadata)
        return kms_client.decrypt(encrypted_credentials, kms_key_id)
