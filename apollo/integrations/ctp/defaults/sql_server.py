from typing import NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig


class SqlServerOdbcArgs(TypedDict):
    # Connection identity — ODBC key names are uppercase by convention
    DRIVER: Required[str]
    SERVER: Required[str]  # "tcp:{host},{port}"
    UID: Required[str]
    PWD: Required[str]
    # Optional connection fields
    DATABASE: NotRequired[str]  # required for Azure variants
    MARS_Connection: NotRequired[str]  # "Yes" — multiple active result sets
    Encrypt: NotRequired[str]  # "yes" / "no" / "strict"
    TrustServerCertificate: NotRequired[str]  # "yes" / "no"


_SQL_SERVER_BASE_FIELD_MAP = {
    "DRIVER": "{ODBC Driver 17 for SQL Server}",
    # SERVER combines host and port in ODBC native format: tcp:{host},{port}
    "SERVER": "tcp:{{ raw.host }},{{ raw.port | default(1433) }}",
    "UID": "{{ raw.user | default(raw.username) }}",
    "PWD": "{{ raw.password }}",
    "MARS_Connection": "Yes",
    # Timeout fields — not ODBC params; proxy clients pop these before building the connection string
    "login_timeout": "{{ raw.login_timeout | default(none) }}",
    "query_timeout_in_seconds": "{{ raw.query_timeout_in_seconds | default(none) }}",
}

SQL_SERVER_DEFAULT_CTP = CtpConfig(
    name="sql-server-default",
    steps=[],
    mapper=MapperConfig(
        name="sql_server_odbc_args",
        schema=SqlServerOdbcArgs,
        field_map=_SQL_SERVER_BASE_FIELD_MAP,
    ),
)

AZURE_SQL_DATABASE_DEFAULT_CTP = CtpConfig(
    name="azure-sql-database-default",
    steps=[],
    mapper=MapperConfig(
        name="azure_sql_database_odbc_args",
        schema=SqlServerOdbcArgs,
        field_map={
            **_SQL_SERVER_BASE_FIELD_MAP,
            "DATABASE": "{{ raw.db_name | default(raw.database) }}",
        },
    ),
)

AZURE_DEDICATED_SQL_POOL_DEFAULT_CTP = CtpConfig(
    name="azure-dedicated-sql-pool-default",
    steps=[],
    mapper=MapperConfig(
        name="azure_dedicated_sql_pool_odbc_args",
        schema=SqlServerOdbcArgs,
        field_map={
            **_SQL_SERVER_BASE_FIELD_MAP,
            "DATABASE": "{{ raw.db_name | default(raw.database) }}",
        },
    ),
)

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("sql-server", SQL_SERVER_DEFAULT_CTP)
CtpRegistry.register("azure-sql-database", AZURE_SQL_DATABASE_DEFAULT_CTP)
CtpRegistry.register("azure-dedicated-sql-pool", AZURE_DEDICATED_SQL_POOL_DEFAULT_CTP)
