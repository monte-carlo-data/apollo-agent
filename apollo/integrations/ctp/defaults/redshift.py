from typing import Any, NotRequired, Required, TypedDict

from apollo.credentials.schema.common import SSL_OPTIONS_FIELD
from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep


class RedshiftClientArgs(TypedDict):
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


# Redshift self-hosted credentials schema. Docs require all five fields but
# the CTP defaults `port` to 5439 and `user` to "awsuser"; the validator
# matches the more forgiving code shape and only requires host/dbname/password.
# Port is documented as a string ("5439") but the connector accepts both
# string and integer; we accept either to avoid spurious type errors.
REDSHIFT_CREDENTIALS_SCHEMA = {
    "connect_args": {
        "type": "dict",
        "required": True,
        "schema": {
            "host": {"type": "string", "required": True, "empty": False},
            "dbname": {"type": "string", "required": True, "empty": False},
            "user": {"type": "string"},  # CTP defaults to "awsuser"
            "password": {"type": "string", "required": True, "empty": False},
            # Port is documented as a string ("5439"); the connector
            # accepts both; CTP defaults to 5439.
            "port": {"type": ["string", "integer"]},
            "connect_timeout": {"type": "integer"},
            "query_timeout_in_seconds": {"type": "integer"},
            "ssl_mode": {"type": "string"},
        },
    },
    "ssl_options": SSL_OPTIONS_FIELD,
    # Top-level autocommit per docs example.
    "autocommit": {"type": "boolean"},
}

REDSHIFT_DEFAULT_CTP = CtpConfig(
    name="redshift-default",
    raw_credentials_schema=REDSHIFT_CREDENTIALS_SCHEMA,
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
        name="redshift_client_args",
        schema=RedshiftClientArgs,
        field_map={
            "host": "{{ raw.host }}",
            "port": "{{ raw.port | default(5439) }}",
            "dbname": "{{ raw.db_name | default(raw.dbname) | default(raw.database) }}",
            "user": "{{ raw.user | default('awsuser') }}",
            "password": "{{ raw.password }}",
            "connect_timeout": "{{ raw.connect_timeout | default(none) }}",
            # statement_timeout in ms; derived from query_timeout_in_seconds when provided
            "options": "{{ '-c statement_timeout=' ~ (raw.query_timeout_in_seconds | int * 1000) if raw.query_timeout_in_seconds is defined else none }}",
            "sslmode": "{{ raw.ssl_mode | default(none) }}",
        },
    ),
    # TCP keepalives required for AWS PrivateLink; injected as defaults so custom
    # CTP configs inherit them without having to redeclare them.
    connect_args_defaults={
        "connect_timeout": 30,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    },
)

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("redshift", REDSHIFT_DEFAULT_CTP)
