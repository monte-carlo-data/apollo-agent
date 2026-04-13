from typing import Any, Dict, Optional

from apollo.integrations.aws.base_aws_proxy_client import BaseAwsProxyClient


class SecretsManagerProxyClient(BaseAwsProxyClient):
    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        BaseAwsProxyClient.__init__(
            self, service_type="secretsmanager", credentials=credentials
        )

    def get_secret_string(self, secret_name: str) -> str | None:
        secret = self.wrapped_client.get_secret_value(SecretId=secret_name)
        return secret.get("SecretString")
