from apollo.credentials.base import BaseCredentialsService
import json

from apollo.integrations.aws.asm_proxy_client import SecretsManagerProxyClient

SECRET_NAME = "aws_secret"


class AwsSecretsManagerCredentialsService(BaseCredentialsService):
    """
    Credentials service that fetches credentials from AWS Secrets Manager.
    """

    def get_credentials(self, credentials: dict) -> dict:
        secret_name = credentials.get(SECRET_NAME)
        if not secret_name:
            raise ValueError("Missing expected secret name in credentials")
        try:
            asm_client = SecretsManagerProxyClient(credentials=credentials)
            secret_str = asm_client.get_secret_string(secret_name)
            if not secret_str:
                raise ValueError(
                    f"Failed to fetch credentials from AWS Secrets Manager: No secret string found for secret name: {secret_name}"
                )
            return json.loads(secret_str)
        except Exception as e:
            raise ValueError(
                f"Failed to fetch credentials from AWS Secrets Manager: {e}"
            )
