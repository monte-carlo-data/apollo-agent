import json
import logging
from typing import Dict, Optional

from apollo.integrations.base_proxy_client import BaseProxyClient


_logger = logging.getLogger(__name__)


class LoadTestProxyClient(BaseProxyClient):
    """
    Proxy client class to perform load tests for the agent.
    It supports an operation to return a result of the specified size.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs):  # type: ignore
        self._credentials = credentials

    @property
    def wrapped_client(self):
        return None

    def execute(
        self,
        result_size_mbs: int,
    ) -> Dict:
        digits = "0123456789"
        kb = digits * 102 + "0123"
        mb = {i: kb for i in range(1024)}
        return {"result": [mb for _ in range(result_size_mbs)]}
