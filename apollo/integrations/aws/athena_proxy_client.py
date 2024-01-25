import json
from typing import Any, Dict, Optional

from apollo.agent.serde import AgentSerializer
from apollo.integrations.aws.base_aws_proxy_client import BaseAwsProxyClient


class AthenaProxyClient(BaseAwsProxyClient):
    def __init__(self, credentials: Optional[Dict], **kwargs: Any):
        BaseAwsProxyClient.__init__(
            self, service_type="athena", credentials=credentials
        )

    def process_result(self, value: Any) -> Any:
        """
        Process the result of the methods on this client before being serialized to JSON.
        Serializes the value to JSON using the AgentSerializer and parses it back from JSON.
        """
        return json.loads(json.dumps(value, cls=AgentSerializer))
