from typing import Optional, Dict

from apollo.agent.models import AgentError
from apollo.integrations.base_proxy_client import BaseProxyClient


def _get_proxy_client_bigquery(credentials: Optional[Dict]) -> BaseProxyClient:
    # import driver modules only when needed
    # in subsequent versions we might not want to bundle all dependencies in a single image
    from apollo.integrations.bigquery.bq_proxy_client import BqProxyClient

    return BqProxyClient(credentials=credentials)


_CLIENT_FACTORY_MAPPING = {
    "bigquery": _get_proxy_client_bigquery,
}


class ProxyClientFactory:
    """
    Factory class used to create the proxy clients for a given connection type.
    Clients are expected to extend :class:`BasedProxyClient` and have a constructor receiving a `credentials` object.
    """

    @classmethod
    def get_proxy_client(
        cls, connection_type: str, credentials: Dict
    ) -> BaseProxyClient:
        factory_method = _CLIENT_FACTORY_MAPPING.get(connection_type)
        if factory_method:
            return factory_method(credentials)
        else:
            raise AgentError(
                f"Connection type not supported by this agent: {connection_type}"
            )
