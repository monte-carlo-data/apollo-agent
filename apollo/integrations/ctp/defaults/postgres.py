from typing import TypedDict, Required, NotRequired

from apollo.credentials.schema.common import SSL_OPTIONS_FIELD
from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep


class PostgresClientArgs(TypedDict):
    # Required connection identifiers
    host: Required[str]
    port: Required[int]
    dbname: Required[str]
    user: Required[str]
    password: Required[str]
    # SSL
    sslmode: NotRequired[str]
    sslrootcert: NotRequired[str]
    sslcert: NotRequired[str]
    sslkey: NotRequired[str]
    sslcrl: NotRequired[str]
    # Connection behavior
    connect_timeout: NotRequired[int]
    application_name: NotRequired[str]
    options: NotRequired[str]
    # TCP keepalives
    keepalives: NotRequired[int]
    keepalives_idle: NotRequired[int]
    keepalives_interval: NotRequired[int]
    keepalives_count: NotRequired[int]
    # Multi-host / HA
    target_session_attrs: NotRequired[str]


# Schema mirrors the docs accordion (dbname/host/port/user/password). The CTP
# also accepts `database` and `db_name` as fallback spellings for legacy DC
# pre-shape paths, but the validator surfaces only the docs spelling so
# customers get a clear "use this field name" message if they typo it.
POSTGRES_CREDENTIALS_SCHEMA = {
    "connect_args": {
        "type": "dict",
        "required": True,
        "schema": {
            "host": {"type": "string", "required": True, "empty": False},
            "port": {"type": "integer", "required": True},
            "dbname": {"type": "string", "required": True, "empty": False},
            "user": {"type": "string", "required": True, "empty": False},
            "password": {"type": "string", "required": True, "empty": False},
            "ssl_mode": {"type": "string"},
        },
    },
    "ssl_options": SSL_OPTIONS_FIELD,
}

POSTGRES_DEFAULT_CTP = CtpConfig(
    name="postgres-default",
    raw_credentials_schema=POSTGRES_CREDENTIALS_SCHEMA,
    steps=[
        TransformStep(
            type="resolve_ssl_options",
            when="raw.ssl_options is defined",
            input={"ssl_options": "{{ raw.ssl_options }}"},
            output={
                "ssl_options": "ssl_options",
                "ca_path": "ssl_ca_path",
            },
            field_map={
                "sslrootcert": "{{ derived.ssl_ca_path if derived.ssl_ca_path is defined else none }}",
                "sslmode": "{{ raw.ssl_mode | default('require') if derived.ssl_ca_path is defined else raw.ssl_mode | default(none) }}",
            },
        )
    ],
    mapper=MapperConfig(
        name="postgres_client_args",
        schema=PostgresClientArgs,
        field_map={
            "host": "{{ raw.host }}",
            "port": "{{ raw.port }}",
            # DC sends driver-native "dbname"; flat credentials use "database" or "db_name"
            "dbname": "{{ raw.database | default(raw.db_name) | default(raw.dbname) }}",
            "user": "{{ raw.user }}",
            "password": "{{ raw.password }}",
            "sslmode": "{{ raw.ssl_mode | default(none) }}",
        },
    ),
    # TCP keepalives required for AWS PrivateLink; injected as defaults so custom
    # CTP configs inherit them without having to redeclare them.
    connect_args_defaults={
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    },
)

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("postgres", POSTGRES_DEFAULT_CTP)
