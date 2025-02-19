import json
from typing import Any, Dict, Optional

from apollo.agent.serde import AgentSerializer, encode_dictionary
from apollo.integrations.aws.base_aws_proxy_client import BaseAwsProxyClient


class S3ProxyClient(BaseAwsProxyClient):
    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        BaseAwsProxyClient.__init__(self, service_type="s3", credentials=credentials)

    def process_result(self, value: Any) -> Any:
        """
        Process the result of the methods on this client before being serialized to JSON.
        Serializes the value to JSON using the AgentSerializer and parses it back from JSON.
        """
        return encode_dictionary(value)
