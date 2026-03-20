from typing import NotRequired, Required, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig


class HiveClientArgs(TypedDict):
    host: Required[str]
    port: Required[str]
    user: NotRequired[str]
    database: NotRequired[str]
    auth_mechanism: NotRequired[str]
    timeout: NotRequired[int]
    use_ssl: NotRequired[bool]


HIVE_DEFAULT_CCP = CcpConfig(
    name="hive-default",
    steps=[],
    mapper=MapperConfig(
        name="hive_client_args",
        schema=HiveClientArgs,
        field_map={
            "host": "{{ raw.host }}",
            "port": "{{ raw.port }}",
            "user": "{{ raw.user | default(none) }}",
            "database": "{{ raw.database | default(none) }}",
            "auth_mechanism": "{{ raw.auth_mechanism | default(none) }}",
            "timeout": "{{ raw.timeout | default(none) }}",
            "use_ssl": "{{ raw.use_ssl | default(none) }}",
        },
    ),
)

from apollo.integrations.ccp.registry import CcpRegistry  # noqa: E402

CcpRegistry.register("hive", HIVE_DEFAULT_CCP)
