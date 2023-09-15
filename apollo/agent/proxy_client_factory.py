from typing import Optional, Dict

from apollo.agent.models import AgentError
from apollo.integrations.base_proxy_client import BaseProxyClient


class ProxyClientFactory:
    @classmethod
    def get_proxy_client(
        cls, connection_type: str, credentials: Dict
    ) -> BaseProxyClient:
        if connection_type == "bigquery":
            return cls._get_proxy_client_bigquery(credentials)
        else:
            raise AgentError(
                f"Connection type not supported by this agent: {connection_type}"
            )

    @staticmethod
    def _get_proxy_client_bigquery(credentials: Optional[Dict]) -> BaseProxyClient:
        # import driver modules only when needed
        # in subsequent versions we might not want to bundle all dependencies in a single image
        from apollo.integrations.bigquery.bq_proxy_client import BqProxyClient

        return BqProxyClient(credentials=credentials)
