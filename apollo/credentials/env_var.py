import os
import json
from apollo.credentials.base import BaseCredentialsService
from apollo.credentials.decryption.factory import CredentialDecryptionFactory

ENV_VAR_NAME = "env_var_name"


class EnvVarCredentialsService(BaseCredentialsService):
    """
    Credentials service that fetches credentials from environment variables
    and decrypts them if necessary.
    """

    def get_credentials(self, credentials: dict) -> dict:
        env_var_name = credentials.get(ENV_VAR_NAME)
        if not env_var_name:
            raise ValueError(
                "Missing expected environment variable name in credentials"
            )
        env_var_credentials = os.getenv(env_var_name)
        if not env_var_credentials:
            raise ValueError(f"Missing expected environment variable: {env_var_name}")
        decryption_service = (
            CredentialDecryptionFactory.get_credentials_decryption_service(credentials)
        )
        env_var_credentials = decryption_service.decrypt(
            env_var_credentials, credentials
        )
        try:
            return json.loads(env_var_credentials)
        except Exception:
            raise ValueError(f"Invalid JSON in environment variable: {env_var_name}")
