import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict

from apollo.agent.models import AgentError
from apollo.integrations.base_proxy_client import BaseProxyClient

logger = logging.getLogger(__name__)

_CACHE_EXPIRATION_SECONDS = int(os.getenv("CLIENT_CACHE_EXPIRATION_SECONDS", "60"))


def _get_proxy_client_bigquery(credentials: Optional[Dict]) -> BaseProxyClient:
    # import driver modules only when needed
    # in subsequent versions we might not want to bundle all dependencies in a single image
    from apollo.integrations.bigquery.bq_proxy_client import BqProxyClient

    return BqProxyClient(credentials=credentials)


def _get_proxy_client_databricks(credentials: Optional[Dict]) -> BaseProxyClient:
    from apollo.integrations.databricks.databricks_sql_warehouse_proxy_client import (
        DatabricksSqlWarehouseProxyClient,
    )

    return DatabricksSqlWarehouseProxyClient(credentials=credentials)


def _get_proxy_client_http(credentials: Optional[Dict]) -> BaseProxyClient:
    from apollo.integrations.http.http_proxy_client import HttpProxyClient

    return HttpProxyClient(credentials=credentials)


@dataclass
class ProxyClientCacheEntry:
    created_time: datetime
    client: BaseProxyClient


_CLIENT_FACTORY_MAPPING = {
    "bigquery": _get_proxy_client_bigquery,
    "databricks": _get_proxy_client_databricks,
    "http": _get_proxy_client_http,
}


class ProxyClientFactory:
    """
    Factory class used to create the proxy clients for a given connection type.
    Clients are expected to extend :class:`BasedProxyClient` and have a constructor receiving a `credentials` object.
    """

    _clients_cache: Dict[str, ProxyClientCacheEntry] = {}

    @classmethod
    def get_proxy_client(
        cls, connection_type: str, credentials: Dict
    ) -> BaseProxyClient:
        try:
            # key = cls._get_cache_key(connection_type, credentials)
            # client = cls._get_cached_client(key)
            # if not client:
            #     client = cls._create_proxy_client(connection_type, credentials)
            #     logger.info(f"Caching {connection_type} client")
            #     cls._cache_client(key, client)
            # return client
            return cls._create_proxy_client(connection_type, credentials)
        except Exception:
            # logger.exception("Failed to create or get client from cache")
            logger.exception("Failed to create client")
            raise

    @classmethod
    def _create_proxy_client(
        cls, connection_type: str, credentials: Dict
    ) -> BaseProxyClient:
        factory_method = _CLIENT_FACTORY_MAPPING.get(connection_type)
        if factory_method:
            return factory_method(credentials)
        else:
            raise AgentError(
                f"Connection type not supported by this agent: {connection_type}"
            )

    @staticmethod
    def _get_cache_key(connection_type: str, credentials: Optional[Dict]) -> str:
        if credentials:
            sha = hashlib.sha256()
            sha.update(bytes(json.dumps(credentials), "utf-8"))
            return f"{connection_type}_{sha.hexdigest()}"
        else:
            return connection_type

    @classmethod
    def _cache_client(cls, key: str, client: BaseProxyClient):
        cls._clients_cache[key] = ProxyClientCacheEntry(datetime.now(), client)

    @classmethod
    def _get_cached_client(cls, key: str) -> Optional[BaseProxyClient]:
        if _CACHE_EXPIRATION_SECONDS <= 0:  # cache disabled
            return None
        entry = cls._clients_cache.get(key)
        if (
            not entry
            or (datetime.now() - entry.created_time).seconds > _CACHE_EXPIRATION_SECONDS
        ):
            return None
        return entry.client
