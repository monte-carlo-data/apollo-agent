from google.cloud.secretmanager_v1 import SecretManagerServiceClient

from apollo.credentials.base import BaseCredentialsService
import json

SECRET_NAME = "gcp_secret"


class GoogleSecretManagerCredentialsService(BaseCredentialsService):
    """
    Credentials service that fetches credentials from GCP Secret Manager.
    """

    def _load_external_credentials(self, credentials: dict) -> dict:
        secret_name = credentials.get(SECRET_NAME)
        if not secret_name:
            raise ValueError("Missing expected secret name 'gcp_secret' in credentials")
        try:
            client = SecretManagerServiceClient()
            response = client.access_secret_version(request={"name": secret_name})
            secret_str = (
                response.payload.data.decode("UTF-8") if response.payload.data else None
            )
            if not secret_str:
                raise ValueError(
                    f"Failed to fetch credentials from GCP Secret Manager: No secret string found for secret name: {secret_name}"
                )
            return json.loads(secret_str)
        except Exception as e:
            raise ValueError(
                f"Failed to fetch credentials from GCP Secret Manager: {e}"
            )
