from typing import TypedDict, Required, NotRequired

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


POSTGRES_DEFAULT_CTP = CtpConfig(
    name="postgres-default",
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
)

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("postgres", POSTGRES_DEFAULT_CTP)
