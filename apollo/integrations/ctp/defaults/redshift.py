from typing import Any, NotRequired, Required, TypedDict

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


REDSHIFT_DEFAULT_CTP = CtpConfig(
    name="redshift-default",
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
            # DC hardcodes these keepalive values for all Redshift connections
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5,
            # statement_timeout in ms; derived from query_timeout_in_seconds when provided
            "options": "{{ '-c statement_timeout=' ~ (raw.query_timeout_in_seconds | int * 1000) if raw.query_timeout_in_seconds is defined else none }}",
            "sslmode": "{{ raw.ssl_mode | default(none) }}",
        },
    ),
)

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("redshift", REDSHIFT_DEFAULT_CTP)
