import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict

from apollo.agent.env_vars import CLIENT_CACHE_EXPIRATION_SECONDS_ENV_VAR
from apollo.agent.models import AgentError
from apollo.integrations.base_proxy_client import BaseProxyClient

logger = logging.getLogger(__name__)


# configure the amount of time connections are cached in memory
# a value < 0 is used to disable caching
_CACHE_EXPIRATION_SECONDS = int(
    os.getenv(CLIENT_CACHE_EXPIRATION_SECONDS_ENV_VAR, "60")
)


def _get_proxy_client_bigquery(
    credentials: Optional[Dict], **kwargs  # type: ignore
) -> BaseProxyClient:
    # import driver modules only when needed
    # in subsequent versions we might not want to bundle all dependencies in a single image
    from apollo.integrations.bigquery.bq_proxy_client import BqProxyClient

    return BqProxyClient(credentials=credentials)


def _get_proxy_client_databricks(
    credentials: Optional[Dict], **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.databricks.databricks_sql_warehouse_proxy_client import (
        DatabricksSqlWarehouseProxyClient,
    )

    return DatabricksSqlWarehouseProxyClient(credentials=credentials)


def _get_proxy_client_http(credentials: Optional[Dict], **kwargs) -> BaseProxyClient:  # type: ignore
    from apollo.integrations.http.http_proxy_client import HttpProxyClient

    return HttpProxyClient(credentials=credentials)


def _get_proxy_client_storage(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.storage.storage_proxy_client import StorageProxyClient

    return StorageProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_looker(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.looker.looker_proxy_client import LookerProxyClient

    return LookerProxyClient(credentials=credentials, platform=platform)


@dataclass
class ProxyClientCacheEntry:
    created_time: datetime
    client: BaseProxyClient


_CLIENT_FACTORY_MAPPING = {
    "bigquery": _get_proxy_client_bigquery,
    "databricks": _get_proxy_client_databricks,
    "http": _get_proxy_client_http,
    "storage": _get_proxy_client_storage,
    "looker": _get_proxy_client_looker,
}


class ProxyClientFactory:
    """
    Factory class used to create the proxy clients for a given connection type.
    Clients are expected to extend :class:`BasedProxyClient` and have a constructor receiving a `credentials` object.
    """

    # cache clients in memory for this instance, clients are cached just for some time as configured by
    # _CACHE_EXPIRATION_SECONDS
    _clients_cache: Dict[str, ProxyClientCacheEntry] = {}

    @classmethod
    def get_proxy_client(
        cls,
        connection_type: str,
        credentials: Optional[Dict],
        skip_cache: bool,
        platform: str,
    ) -> BaseProxyClient:
        # skip_cache is a flag sent by the client, and can be used to force a new client to be created
        # it defaults to False
        if skip_cache:
            logger.info(f"Client cache for {connection_type} skipped")
            try:
                return cls._create_proxy_client(connection_type, credentials, platform)
            except Exception:
                logger.exception(f"Failed to create {connection_type} client")
                raise

        try:
            # create a cache key to search/store the client in cache, it uses the connection type and
            # a hash value derived from the credentials object
            key = cls._get_cache_key(connection_type, credentials)

            # get a non expired client
            client = cls._get_cached_client(key)
            if not client:
                client = cls._create_proxy_client(
                    connection_type, credentials, platform
                )
                logger.info(f"Caching {connection_type} client")
                cls._cache_client(key, client)
            return client
        except Exception:
            logger.exception("Failed to create or get client from cache")
            raise

    @classmethod
    def _create_proxy_client(
        cls, connection_type: str, credentials: Optional[Dict], platform: str
    ) -> BaseProxyClient:
        factory_method = _CLIENT_FACTORY_MAPPING.get(connection_type)
        if factory_method:
            return factory_method(credentials, platform=platform)
        else:
            raise AgentError(
                f"Connection type not supported by this agent: {connection_type}"
            )

    @staticmethod
    def _get_cache_key(connection_type: str, credentials: Optional[Dict]) -> str:
        """
        Returns a cache key used to cache a client for the given connection type and credentials.
        The key is calculated by concatenating the connection type with a sha-256 hash derived from the credentials
        object.
        :param connection_type:
        :param credentials:
        :return:
        """
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

        # check that entry has not expired
        if (
            not entry
            or (datetime.now() - entry.created_time).seconds > _CACHE_EXPIRATION_SECONDS
        ):
            return None
        return entry.client
