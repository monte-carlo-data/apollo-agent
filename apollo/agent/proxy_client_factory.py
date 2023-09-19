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


@dataclass
class ProxyClientCacheEntry:
    created_time: datetime
    client: BaseProxyClient


class ProxyClientFactory:
    _clients_cache: Dict[str, ProxyClientCacheEntry] = {}

    @classmethod
    def get_proxy_client(
        cls, connection_type: str, credentials: Dict
    ) -> BaseProxyClient:
        try:
            key = cls._get_cache_key(connection_type, credentials)
            client = cls._get_cached_client(key)
            if not client:
                client = cls._create_proxy_client(connection_type, credentials)
                logger.info(f"Caching {connection_type} client")
                cls._cache_client(key, client)
            return client
        except Exception:
            logger.exception("Failed to create or get client from cache")
            raise

    @classmethod
    def _create_proxy_client(
        cls, connection_type: str, credentials: Dict
    ) -> BaseProxyClient:
        if connection_type == "bigquery":
            return cls._get_proxy_client_bigquery(credentials)
        elif connection_type == "gcs":
            return cls._get_proxy_client_gcs(credentials)
        elif connection_type == "databricks":
            return cls._get_proxy_client_databricks(credentials)
        elif connection_type == "http":
            return cls._get_proxy_client_http(credentials)
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

    @staticmethod
    def _get_proxy_client_bigquery(credentials: Optional[Dict]) -> BaseProxyClient:
        # import driver modules only when needed
        # in subsequent versions we might not want to bundle all dependencies in a single image
        from apollo.integrations.bigquery.bq_proxy_client import BqProxyClient

        return BqProxyClient(credentials=credentials)

    @staticmethod
    def _get_proxy_client_gcs(credentials: Optional[Dict]) -> BaseProxyClient:
        # import driver modules only when needed
        # in subsequent versions we might not want to bundle all dependencies in a single image
        from apollo.integrations.gcs.gcs_proxy_client import GcsProxyClient

        return GcsProxyClient(credentials=credentials)

    @staticmethod
    def _get_proxy_client_databricks(credentials: Optional[Dict]) -> BaseProxyClient:
        # import driver modules only when needed
        # in subsequent versions we might not want to bundle all dependencies in a single image
        from apollo.integrations.databricks.databricks_sql_warehouse_proxy_client import (
            DatabricksSqlWarehouseProxyClient,
        )

        return DatabricksSqlWarehouseProxyClient(credentials=credentials)

    @staticmethod
    def _get_proxy_client_http(credentials: Optional[Dict]) -> BaseProxyClient:
        # import driver modules only when needed
        # in subsequent versions we might not want to bundle all dependencies in a single image
        from apollo.integrations.http.http_proxy_client import HttpProxyClient

        return HttpProxyClient(credentials=credentials)
