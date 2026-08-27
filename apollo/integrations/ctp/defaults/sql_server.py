from typing import NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep


class SqlServerOdbcArgs(TypedDict):
    # Connection identity — ODBC key names are uppercase by convention
    DRIVER: Required[str]
    SERVER: Required[str]  # "tcp:{host},{port}"
    # UID/PWD are NotRequired because the kerberos (Windows auth) path omits both by
    # design -- Active Directory vouches for the client and no credential goes on the
    # wire. The mapper enforces __required_keys__ at runtime, so leaving these Required
    # would make the kerberos path fail validation. Presence on the sql path is still
    # enforced upstream by SQL_SERVER_CREDENTIALS_SCHEMA.
    UID: NotRequired[str]
    PWD: NotRequired[str]
    Trusted_Connection: NotRequired[str]  # "yes" — kerberos / Windows authentication
    # Optional connection fields
    DATABASE: NotRequired[str]  # required for Azure variants
    Authentication: NotRequired[str]  # "ActiveDirectoryServicePrincipal"
    MARS_Connection: NotRequired[str]  # "Yes" — multiple active result sets
    Encrypt: NotRequired[str]  # "yes" / "no" / "strict"
    TrustServerCertificate: NotRequired[str]  # "yes" / "no"
    # Not ODBC params. The base field map emits them and SqlServerProxyClient pops both
    # off connect_args, so the schema has to declare them or the mapper rejects its own
    # output.
    login_timeout: NotRequired[int]
    query_timeout_in_seconds: NotRequired[int]
    # Kerberos artefact locations, popped the same way. A dict, not a string: it is
    # consumed as process environment, never serialized into the connection string.
    kerberos: NotRequired[dict]


_SQL_SERVER_BASE_FIELD_MAP = {
    "DRIVER": "{ODBC Driver 17 for SQL Server}",
    # SERVER combines host and port in ODBC native format: tcp:{host},{port}
    "SERVER": "tcp:{{ raw.host }},{{ raw.port | default(1433) }}",
    # Inner default: Jinja evaluates the argument eagerly, so without it a credential
    # carrying neither field raises "'username' is undefined" instead of yielding nothing.
    "UID": "{{ raw.user | default(raw.username | default(none)) }}",
    "PWD": "{{ raw.password }}",
    # Timeout fields — not ODBC params; proxy clients pop these before building the connection string
    "login_timeout": "{{ raw.login_timeout | default(none) }}",
    # The collector sends `query_timeout`; accept both so an override is not silently
    # dropped back to the 840s default that happens to match on each side.
    "query_timeout_in_seconds": (
        "{{ raw.query_timeout_in_seconds | default(raw.query_timeout | default(none)) }}"
    ),
}

# Windows Authentication (Kerberos) — PRO-3016.
#
# Guarded by `when`, so it runs only for auth_type=kerberos and every existing
# username/password connection is untouched. Because a step's field_map is applied only
# when the step executes, and the mapper drops None values, this is also how UID and PWD
# get *removed* on the kerberos path — the entire connection-string change is
# Trusted_Connection=yes with no credential on the wire.
_KERBEROS_STEP = TransformStep(
    type="prepare_kerberos",
    when="raw.auth_type is defined and raw.auth_type | lower == 'kerberos'",
    input={
        "realm": "{{ raw.realm | default(none) }}",
        "kdc": "{{ raw.kdc | default(none) }}",
        "principal": "{{ raw.principal | default(none) }}",
        "keytab_base64": "{{ raw.keytab_base64 | default(none) }}",
        "password": "{{ raw.password | default(none) }}",
    },
    output={"kerberos": "kerberos_env"},
    field_map={
        "Trusted_Connection": "yes",
        # Nested-dict passthrough, following Oracle's ssl_options: SqlServerProxyClient
        # pops this and sets the KRB5_* variables around pyodbc.connect. They are
        # process-global and shared with Hive/Impala GSSAPI, so they cannot be set here.
        "kerberos": "{{ derived.kerberos_env }}",
        # None removes the base field map's entry: AD vouches for the client, so no
        # credential is sent. Leaving PWD would additionally offer the AD service
        # account's password to SQL Server as a SQL login — the very thing customers
        # requiring Windows auth have banned.
        "UID": None,
        "PWD": None,
    },
)

SQL_SERVER_DEFAULT_CTP = CtpConfig(
    name="sql-server-default",
    steps=[_KERBEROS_STEP],
    mapper=MapperConfig(
        name="sql_server_odbc_args",
        schema=SqlServerOdbcArgs,
        field_map={
            **_SQL_SERVER_BASE_FIELD_MAP,
            "MARS_Connection": "Yes",
            # The collector sends `database` in connect_args on the kerberos path. Omitting
            # it here dropped the value silently and landed the session in master; the
            # legacy sql path never surfaced this because it sent a pre-built ODBC string.
            "DATABASE": "{{ raw.database | default(raw.db_name | default(none)) }}",
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
# Auth modes are mutually exclusive via oneof_schema, following the databricks and
# informatica_v2 precedent. Without this the kerberos form would be rejected outright,
# because the sql form requires user and password -- and the self-hosted credential JSON
# is exactly how a Windows-auth connection is configured.
_SQL_SERVER_HOST_FIELDS = {
    "host": {"type": "string", "required": True, "empty": False},
    # Both types: the collector types port as a string (PluginConnectionSchema.port is
    # Optional[str]) while a customer hand-writing self-hosted JSON writes an integer.
    # Two producers, one schema. The mapper interpolates it into SERVER either way.
    # Matches the collector's own Redshift schema, which already accepts both.
    "port": {"type": ["string", "integer"]},
    "database": {"type": "string"},
}

_SQL_SERVER_KERBEROS_FIELDS = {
    **_SQL_SERVER_HOST_FIELDS,
    # host must be the SQL Server FQDN here: the ODBC driver derives the SPN as
    # MSSQLSvc/<host>:<port> and does not support ServerSPN, so an IP or a CNAME that
    # does not match the registered SPN cannot be compensated for in code.
    "auth_type": {"type": "string", "required": True, "allowed": ["kerberos"]},
    "realm": {"type": "string", "required": True, "empty": False},
    "kdc": {"type": "string", "required": True, "empty": False},
    "principal": {"type": "string", "required": True, "empty": False},
}

_SQL_SERVER_CONNECT_ARGS_DICT_SCHEMA = {
    "type": "dict",
    "oneof_schema": [
        # SQL Authentication — unchanged; every existing customer rides this.
        {
            **_SQL_SERVER_HOST_FIELDS,
            "auth_type": {"type": "string", "allowed": ["sql"]},
            "user": {"type": "string", "required": True, "empty": False},
            "password": {"type": "string", "required": True, "empty": False},
        },
        # Windows Authentication, keytab form (recommended).
        {
            **_SQL_SERVER_KERBEROS_FIELDS,
            "keytab_base64": {"type": "string", "required": True, "empty": False},
        },
        # Windows Authentication, password form — for customers who cannot readily
        # produce a keytab. oneof_schema also gives the keytab/password mutual exclusion
        # for free: allow_unknown propagates, so supplying both satisfies two variants and
        # oneof requires exactly one.
        {
            **_SQL_SERVER_KERBEROS_FIELDS,
            "password": {"type": "string", "required": True, "empty": False},
        },
    ],
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
