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
    Authentication: NotRequired[str]  # "ActiveDirectoryServicePrincipal"
    MARS_Connection: NotRequired[str]  # "Yes" — multiple active result sets
    Encrypt: NotRequired[str]  # "yes" / "no" / "strict"
    TrustServerCertificate: NotRequired[str]  # "yes" / "no"


_SQL_SERVER_BASE_FIELD_MAP = {
    "DRIVER": "{ODBC Driver 17 for SQL Server}",
    # SERVER combines host and port in ODBC native format: tcp:{host},{port}
    "SERVER": "tcp:{{ raw.host }},{{ raw.port | default(1433) }}",
    "UID": "{{ raw.user | default(raw.username) }}",
    "PWD": "{{ raw.password }}",
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
        field_map={
            **_SQL_SERVER_BASE_FIELD_MAP,
            "MARS_Connection": "Yes",
        },
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
            "MARS_Connection": "Yes",
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
            "MARS_Connection": "Yes",
            "DATABASE": "{{ raw.db_name | default(raw.database) }}",
        },
    ),
)


MS_FABRIC_DEFAULT_CTP = CtpConfig(
    name="microsoft-fabric-default",
    steps=[],
    mapper=MapperConfig(
        name="ms_fabric_odbc_args",
        schema=SqlServerOdbcArgs,
        field_map={
            **_SQL_SERVER_BASE_FIELD_MAP,
            "DATABASE": "{{ raw.database | default(raw.db_name) }}",
            "Authentication": "ActiveDirectoryServicePrincipal",
            "UID": "{{ raw.client_id }}@{{ raw.tenant_id }}",
            "PWD": "{{ raw.client_secret }}",
            "Encrypt": "yes",
            "TrustServerCertificate": "no",
        },
    ),
)

# Schemas for the customer-facing self-hosted credentials JSON. The docs
# document `connect_args` as a STRING (a pre-built ODBC connection string);
# the CTP also accepts a structured dict. The validator accepts both — when
# a string, only "non-empty" is enforced (no parsing); when a dict, full
# field-level validation runs. See the follow-up tracked in the plan for
# docs eventually preferring the dict example.
_SQL_SERVER_CONNECT_ARGS_DICT_SCHEMA = {
    "type": "dict",
    "schema": {
        "host": {"type": "string", "required": True, "empty": False},
        "port": {"type": "integer"},
        "user": {"type": "string", "required": True, "empty": False},
        "password": {"type": "string", "required": True, "empty": False},
        "database": {"type": "string"},
    },
    "allow_unknown": True,
}

_AZURE_SQL_CONNECT_ARGS_DICT_SCHEMA = {
    "type": "dict",
    "schema": {
        "host": {"type": "string", "required": True, "empty": False},
        "port": {"type": "integer"},
        "user": {"type": "string", "required": True, "empty": False},
        "password": {"type": "string", "required": True, "empty": False},
        "database": {"type": "string", "required": True, "empty": False},
    },
    "allow_unknown": True,
}


def _string_or_dict(dict_schema: dict) -> dict:
    """Customer can supply connect_args as a pre-built ODBC string OR a dict."""
    return {
        "required": True,
        "anyof": [
            {"type": "string", "empty": False},
            dict_schema,
        ],
    }


SQL_SERVER_CREDENTIALS_SCHEMA = {
    "connect_args": _string_or_dict(_SQL_SERVER_CONNECT_ARGS_DICT_SCHEMA),
    "login_timeout": {"type": "integer"},
    "query_timeout_in_seconds": {"type": "integer"},
    "query_timeout": {"type": "integer"},  # docs example uses this spelling
}

AZURE_SQL_DATABASE_CREDENTIALS_SCHEMA = {
    "connect_args": _string_or_dict(_AZURE_SQL_CONNECT_ARGS_DICT_SCHEMA),
    "login_timeout": {"type": "integer"},
    "query_timeout": {"type": "integer"},
}

AZURE_DEDICATED_SQL_POOL_CREDENTIALS_SCHEMA = {
    "connect_args": _string_or_dict(_AZURE_SQL_CONNECT_ARGS_DICT_SCHEMA),
    "login_timeout": {"type": "integer"},
    "query_timeout": {"type": "integer"},
}

# Fabric is dict-only — there is no legacy ODBC-string path here. The customer
# supplies the structured fields and the proxy client builds the ODBC string.
MS_FABRIC_CREDENTIALS_SCHEMA = {
    "connect_args": {
        "type": "dict",
        "required": True,
        "schema": {
            "server": {"type": "string", "required": True, "empty": False},
            "port": {"type": ["string", "integer"]},
            "database": {"type": "string", "required": True, "empty": False},
            "tenant_id": {"type": "string", "required": True, "empty": False},
            "client_id": {"type": "string", "required": True, "empty": False},
            "client_secret": {"type": "string", "required": True, "empty": False},
        },
    },
}


# Attach schemas to the existing CtpConfig instances.
SQL_SERVER_DEFAULT_CTP.raw_credentials_schema = SQL_SERVER_CREDENTIALS_SCHEMA
AZURE_SQL_DATABASE_DEFAULT_CTP.raw_credentials_schema = (
    AZURE_SQL_DATABASE_CREDENTIALS_SCHEMA
)
AZURE_DEDICATED_SQL_POOL_DEFAULT_CTP.raw_credentials_schema = (
    AZURE_DEDICATED_SQL_POOL_CREDENTIALS_SCHEMA
)
MS_FABRIC_DEFAULT_CTP.raw_credentials_schema = MS_FABRIC_CREDENTIALS_SCHEMA


from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("sql-server", SQL_SERVER_DEFAULT_CTP)
CtpRegistry.register("azure-sql-database", AZURE_SQL_DATABASE_DEFAULT_CTP)
CtpRegistry.register("azure-dedicated-sql-pool", AZURE_DEDICATED_SQL_POOL_DEFAULT_CTP)
CtpRegistry.register("microsoft-fabric", MS_FABRIC_DEFAULT_CTP)
