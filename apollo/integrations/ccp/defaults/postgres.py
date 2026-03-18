from typing import TypedDict, Required, NotRequired

from apollo.integrations.ccp.models import CcpConfig, MapperConfig, TransformStep


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


POSTGRES_DEFAULT_CCP = CcpConfig(
    name="postgres-default",
    steps=[
        TransformStep(
            type="tmp_file_write",
            when="raw.ssl_ca_pem is defined",
            input={"contents": "{{ raw.ssl_ca_pem }}", "file_suffix": ".pem", "mode": "0400"},
            output={"path": "ssl_ca_path"},
            field_map={
                "sslrootcert": "{{ derived.ssl_ca_path }}",
                "sslmode": "{{ raw.ssl_mode | default('require') }}",
            },
        )
    ],
    mapper=MapperConfig(
        name="postgres_client_args",
        schema=PostgresClientArgs,
        field_map={
            "host": "{{ raw.host }}",
            "port": "{{ raw.port }}",
            "dbname": "{{ raw.database }}",
            "user": "{{ raw.user }}",
            "password": "{{ raw.password }}",
            "sslmode": "{{ raw.ssl_mode | default(none) }}",
        },
    ),
)

from apollo.integrations.ccp.registry import CcpRegistry

CcpRegistry.register("postgres", POSTGRES_DEFAULT_CCP)
