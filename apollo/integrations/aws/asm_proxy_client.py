from typing import Any, Dict, Optional

from apollo.integrations.aws.base_aws_proxy_client import BaseAwsProxyClient


class SecretsManagerProxyClient(BaseAwsProxyClient):
    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        # SecretsManagerProxyClient is a credentials provider, not an integration
        # connection. Its AWS session params (assumable_role, aws_region, etc.) are
        # always at the top level of the credentials dict. connect_args, if present,
        # holds downstream integration credentials — not ASM session params.
        self._client = self.create_boto_client(
            service_type="secretsmanager",
            assumable_role=credentials.get("assumable_role") if credentials else None,
            aws_region=credentials.get("aws_region") if credentials else None,
            external_id=credentials.get("external_id") if credentials else None,
            ssl_options=credentials.get("ssl_options") if credentials else None,
        )

    def get_secret_string(self, secret_name: str) -> str | None:
        secret = self.wrapped_client.get_secret_value(SecretId=secret_name)
        return secret.get("SecretString")
