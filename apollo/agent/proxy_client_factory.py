import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict

from apollo.agent.constants import (
    CONNECTION_TYPE_BIGQUERY,
    CONNECTION_TYPE_DATABRICKS,
    CONNECTION_TYPE_HTTP,
    CONNECTION_TYPE_S3,
    CONNECTION_TYPE_STORAGE,
    CONNECTION_TYPE_LOOKER,
    CONNECTION_TYPE_GIT,
    CONNECTION_TYPE_REDSHIFT,
    CONNECTION_TYPE_POSTGRES,
    CONNECTION_TYPE_SQL_SERVER,
    CONNECTION_TYPE_SNOWFLAKE,
    CONNECTION_TYPE_MYSQL,
    CONNECTION_TYPE_ORACLE,
    CONNECTION_TYPE_TERADATA,
    CONNECTION_TYPE_AZURE_DEDICATED_SQL_POOL,
    CONNECTION_TYPE_AZURE_SQL_DATABASE,
    CONNECTION_TYPE_TABLEAU,
    CONNECTION_TYPE_SAP_HANA,
    CONNECTION_TYPE_POWER_BI,
    CONNECTION_TYPE_GLUE,
    CONNECTION_TYPE_ATHENA,
    CONNECTION_TYPE_PRESTO,
    CONNECTION_TYPE_HIVE,
    CONNECTION_TYPE_MSK_CONNECT,
    CONNECTION_TYPE_MSK_KAFKA,
    CONNECTION_TYPE_MOTHERDUCK,
    CONNECTION_TYPE_DREMIO,
)
from apollo.agent.env_vars import CLIENT_CACHE_EXPIRATION_SECONDS_ENV_VAR
from apollo.agent.models import AgentError
from apollo.agent.serde import decode_dictionary
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


def _get_proxy_client_s3(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.aws.s3_proxy_client import (
        S3ProxyClient,
    )

    return S3ProxyClient(credentials=credentials, platform=platform)


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


def _get_proxy_client_git(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.git.git_proxy_client import GitProxyClient

    return GitProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_redshift(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.redshift.redshift_proxy_client import RedshiftProxyClient

    return RedshiftProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_postgres(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.db.postgres_proxy_client import PostgresProxyClient

    return PostgresProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_sql_server(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.db.sql_server_proxy_client import SqlServerProxyClient

    return SqlServerProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_snowflake(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.snowflake.snowflake_proxy_client import (
        SnowflakeProxyClient,
    )

    return SnowflakeProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_mysql(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.db.mysql_proxy_client import MysqlProxyClient

    return MysqlProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_oracle(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.db.oracle_proxy_client import OracleProxyClient

    return OracleProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_teradata(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.db.teradata_proxy_client import TeradataProxyClient

    return TeradataProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_azure_database(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.db.azure_database_proxy_client import (
        AzureDatabaseProxyClient,
    )

    return AzureDatabaseProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_sap_hana(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.db.sap_hana_proxy_client import (
        SAPHanaProxyClient,
    )

    return SAPHanaProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_motherduck(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.db.motherduck_proxy_client import (
        MotherDuckProxyClient,
    )

    return MotherDuckProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_tableau(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.tableau.tableau_proxy_client import (
        TableauProxyClient,
    )

    return TableauProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_power_bi(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.powerbi.powerbi_proxy_client import (
        PowerBiProxyClient,
    )

    return PowerBiProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_glue(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.aws.glue_proxy_client import (
        GlueProxyClient,
    )

    return GlueProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_athena(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.aws.athena_proxy_client import (
        AthenaProxyClient,
    )

    return AthenaProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_presto(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.db.presto_proxy_client import (
        PrestoProxyClient,
    )

    return PrestoProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_hive(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.db.hive_proxy_client import (
        HiveProxyClient,
    )

    return HiveProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_msk_connect(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.aws.msk_proxy_client import MskConnectProxyClient

    return MskConnectProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_msk_kafka(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.aws.msk_proxy_client import MskKafkaProxyClient

    return MskKafkaProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_dremio(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.db.dremio_proxy_client import (
        DremioProxyClient,
    )

    return DremioProxyClient(credentials=credentials, platform=platform)


def _get_proxy_client_load_test(
    credentials: Optional[Dict], platform: str, **kwargs  # type: ignore
) -> BaseProxyClient:
    from apollo.integrations.test.load_test_client import (
        LoadTestProxyClient,
    )

    return LoadTestProxyClient(credentials=credentials, platform=platform)


@dataclass
class ProxyClientCacheEntry:
    created_time: datetime
    client: BaseProxyClient


_CLIENT_FACTORY_MAPPING = {
    CONNECTION_TYPE_BIGQUERY: _get_proxy_client_bigquery,
    CONNECTION_TYPE_DATABRICKS: _get_proxy_client_databricks,
    CONNECTION_TYPE_HTTP: _get_proxy_client_http,
    CONNECTION_TYPE_S3: _get_proxy_client_s3,
    CONNECTION_TYPE_STORAGE: _get_proxy_client_storage,
    CONNECTION_TYPE_LOOKER: _get_proxy_client_looker,
    CONNECTION_TYPE_GIT: _get_proxy_client_git,
    CONNECTION_TYPE_REDSHIFT: _get_proxy_client_redshift,
    CONNECTION_TYPE_POSTGRES: _get_proxy_client_postgres,
    CONNECTION_TYPE_SQL_SERVER: _get_proxy_client_sql_server,
    CONNECTION_TYPE_SNOWFLAKE: _get_proxy_client_snowflake,
    CONNECTION_TYPE_MYSQL: _get_proxy_client_mysql,
    CONNECTION_TYPE_ORACLE: _get_proxy_client_oracle,
    CONNECTION_TYPE_TERADATA: _get_proxy_client_teradata,
    CONNECTION_TYPE_AZURE_DEDICATED_SQL_POOL: _get_proxy_client_azure_database,
    CONNECTION_TYPE_AZURE_SQL_DATABASE: _get_proxy_client_azure_database,
    CONNECTION_TYPE_TABLEAU: _get_proxy_client_tableau,
    CONNECTION_TYPE_SAP_HANA: _get_proxy_client_sap_hana,
    CONNECTION_TYPE_POWER_BI: _get_proxy_client_power_bi,
    CONNECTION_TYPE_GLUE: _get_proxy_client_glue,
    CONNECTION_TYPE_ATHENA: _get_proxy_client_athena,
    CONNECTION_TYPE_PRESTO: _get_proxy_client_presto,
    CONNECTION_TYPE_HIVE: _get_proxy_client_hive,
    CONNECTION_TYPE_MSK_CONNECT: _get_proxy_client_msk_connect,
    CONNECTION_TYPE_MSK_KAFKA: _get_proxy_client_msk_kafka,
    CONNECTION_TYPE_MOTHERDUCK: _get_proxy_client_motherduck,
    CONNECTION_TYPE_DREMIO: _get_proxy_client_dremio,
    "load_test": _get_proxy_client_load_test,
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
    def dispose_proxy_client(
        cls,
        connection_type: str,
        credentials: Optional[Dict],
        skip_cache: bool,
    ):
        if skip_cache:
            return
        key = cls._get_cache_key(connection_type, credentials)
        cls._dispose_cached_client(key)
        logger.info(f"Discarded {connection_type} client")

    @classmethod
    def _create_proxy_client(
        cls, connection_type: str, credentials: Optional[Dict], platform: str
    ) -> BaseProxyClient:
        factory_method = _CLIENT_FACTORY_MAPPING.get(connection_type)
        if factory_method:
            if credentials:
                credentials = decode_dictionary(credentials)
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
            # dispose client and connection, so we don't have two connections open at the same time
            if entry:
                cls._dispose_cached_client(key)
            return None
        return entry.client

    @classmethod
    def _dispose_cached_client(cls, key: str):
        entry = cls._clients_cache.pop(key, None)
        if entry:
            logger.info("Closing cached client")
            entry.client.close()
