from typing import Any, Dict, Optional

from apollo.integrations.aws.base_aws_proxy_client import BaseAwsProxyClient


class MskConnectProxyClient(BaseAwsProxyClient):
    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        BaseAwsProxyClient.__init__(
            self, service_type="kafkaconnect", credentials=credentials
        )
