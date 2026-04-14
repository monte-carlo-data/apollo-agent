from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig
from apollo.integrations.ctp.registry import CtpRegistry


class HiveClientArgs(TypedDict):
    # Network
    host: Required[str]
    port: NotRequired[int]  # default 21050
    timeout: NotRequired[int]
    # Database
    database: NotRequired[str]
    # Auth
    auth_mechanism: NotRequired[str]  # NOSASL | PLAIN | GSSAPI | LDAP | JWT
    user: NotRequired[str]
    password: NotRequired[str]
    # Kerberos
    kerberos_service_name: NotRequired[str]  # default "impala"
    krb_host: NotRequired[str]
    # SSL
    use_ssl: NotRequired[bool]
    ca_cert: NotRequired[str]  # path to CA cert file
    # HTTP transport
    use_http_transport: NotRequired[bool]
    http_path: NotRequired[str]
    http_cookie_names: NotRequired[list]
    user_agent: NotRequired[str]
    # JWT
    jwt: NotRequired[str]
    # Retry
    retries: NotRequired[int]  # default 3
    # Extensibility
    get_user_custom_headers_func: NotRequired[Any]


HIVE_DEFAULT_CTP = CtpConfig(
    name="hive-default",
    steps=[],
    mapper=MapperConfig(
        name="hive_client_args",
        schema=HiveClientArgs,
        field_map={
            "host": "{{ raw.host }}",
            "port": "{{ raw.port | default(none) }}",
            "database": "{{ raw.database | default(none) }}",
            "timeout": "{{ raw.timeout | default(none) }}",
            "auth_mechanism": "{{ raw.auth_mechanism | default(none) }}",
            "user": "{{ raw.user | default(none) }}",
            "password": "{{ raw.password | default(none) }}",
            "use_ssl": "{{ raw.use_ssl | default(none) }}",
        },
    ),
)


CtpRegistry.register("hive", HIVE_DEFAULT_CTP)
