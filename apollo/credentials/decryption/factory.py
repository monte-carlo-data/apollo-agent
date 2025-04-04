from apollo.credentials.decryption.base import BaseCredentialDecryptionService
from apollo.credentials.decryption.kms import KmsCredentialDecryptionService

DECRYPTION_SERVICE_TYPE = "decryption_service_type"
DECRYPTION_SERVICE_TYPES = {
    "kms": KmsCredentialDecryptionService,
}


class CredentialDecryptionFactory:
    """
    Factory class used to decrypt credentials using different methods,
    for example when self-hosted credentials are stored in env vars.
    """

    @staticmethod
    def get_credentials_decryption_service(
        credentials: dict,
    ) -> BaseCredentialDecryptionService:
        decryption_service_type = credentials.get(DECRYPTION_SERVICE_TYPE)
        if (
            decryption_service_type
            and decryption_service_type in DECRYPTION_SERVICE_TYPES
        ):
            return DECRYPTION_SERVICE_TYPES[decryption_service_type]()
        elif decryption_service_type:
            raise ValueError(
                f"Invalid decryption service type: {decryption_service_type}. Supported types: {DECRYPTION_SERVICE_TYPES.keys()}"
            )
        return BaseCredentialDecryptionService()
