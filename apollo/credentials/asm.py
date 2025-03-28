from apollo.credentials.base import BaseCredentialsService
import boto3
import json

SECRET_NAME = "secret"
REGION = "region"
ROLE_ARN = "role_arn"
EXTERNAL_ID = "external_id"


class AwsSecretsManagerCredentialsService(BaseCredentialsService):
    """
    Credentials service that fetches credentials from AWS Secrets Manager.
    """

    def get_credentials(self, credentials: dict) -> dict:
        secret_name = credentials.get(SECRET_NAME)
        if not secret_name:
            raise ValueError("Missing expected secret name in credentials")
        # optional parameters:
        region = credentials.get(REGION)
        role_arn = credentials.get(ROLE_ARN)
        external_id = credentials.get(EXTERNAL_ID)
        try:
            asm_client = boto3.client(
                "secretsmanager",
                region_name=region,
                aws_access_key_id=role_arn,
                aws_secret_access_key=external_id,
            )
            secret = asm_client.get_secret_value(SecretId=secret_name)
            secret_str = secret.get("SecretString")
            return json.loads(secret_str)
        except Exception as e:
            raise ValueError(
                f"Failed to fetch credentials from AWS Secrets Manager: {e}"
            )
