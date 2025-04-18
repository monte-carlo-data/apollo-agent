from typing import Any, Dict, Optional
import base64

from apollo.integrations.aws.base_aws_proxy_client import BaseAwsProxyClient


class KmsProxyClient(BaseAwsProxyClient):
    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        BaseAwsProxyClient.__init__(self, service_type="kms", credentials=credentials)

    def decrypt(self, encrypted_credentials: str | bytes, kms_key_id: str) -> str:
        try:
            if isinstance(encrypted_credentials, str):
                encrypted_credentials = base64.b64decode(encrypted_credentials)
        except Exception:
            raise ValueError("Failed to decode base64 encrypted credentials")
        try:
            decrypted_credentials = self.wrapped_client.decrypt(
                CiphertextBlob=encrypted_credentials,
                KeyId=kms_key_id,
            )
            return decrypted_credentials["Plaintext"].decode("utf-8")
        except Exception as e:
            raise ValueError(f"Failed to decrypt credentials using KMS: {e}")
