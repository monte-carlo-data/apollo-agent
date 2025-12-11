from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from apollo.credentials.base import BaseCredentialsService
import json

VAULT_URL = "akv_vault_url"
VAULT_NAME = "akv_vault_name"
SECRET_NAME = "akv_secret"


class AzureKeyVaultCredentialsService(BaseCredentialsService):
    """
    Credentials service that fetches credentials from a secret in Azure Key Vault.
    """

    def _load_external_credentials(self, credentials: dict) -> dict:
        secret_name = credentials.get(SECRET_NAME)
        if not secret_name:
            raise ValueError(f"Missing expected secret name '{SECRET_NAME}' in credentials")
        vault_url = credentials.get(VAULT_URL)
        if not vault_url:
            vault_name = credentials.get(VAULT_NAME)
            if not vault_name:
                raise ValueError(
                    f"One of '{VAULT_URL}' or '{VAULT_NAME}' is required in credentials"
                )
            vault_url = f"https://{vault_name}.vault.azure.net/"
        try:
            client = SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())  # type: ignore
            retrieved_secret = client.get_secret(secret_name)
            secret_str = retrieved_secret.value
            if not secret_str:
                raise ValueError(
                    f"Failed to fetch credentials from Azure Key Vault: No secret string found for secret name: {secret_name}"
                )
            return json.loads(secret_str)
        except Exception as e:
            raise ValueError(f"Failed to fetch credentials from Azure Key Vault: {e}")
