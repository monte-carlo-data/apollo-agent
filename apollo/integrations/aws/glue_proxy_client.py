from typing import Any, Dict, Optional

from apollo.integrations.aws.base_aws_proxy_client import BaseAwsProxyClient


class GlueProxyClient(BaseAwsProxyClient):
    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        BaseAwsProxyClient.__init__(self, service_type="glue", credentials=credentials)
