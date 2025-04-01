class BaseCredentialDecryptionService:
    """
    Base class for credential decryption services, provides default behavior of
    returning the received credentials without any decryption.
    """

    def decrypt(self, encrypted_credentials: str, credential_metadata: dict) -> str:
        return encrypted_credentials
