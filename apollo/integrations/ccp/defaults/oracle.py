from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig


class OracleClientArgs(TypedDict):
    dsn: Required[str]
    user: Required[str]
    password: Required[str]
    expire_time: NotRequired[int]
    ssl_context: NotRequired[Any]


ORACLE_DEFAULT_CCP = CcpConfig(
    name="oracle-default",
    steps=[],
    mapper=MapperConfig(
        name="oracle_client_args",
        schema=OracleClientArgs,
        field_map={
            "dsn": "{{ raw.dsn }}",
            "user": "{{ raw.user }}",
            "password": "{{ raw.password }}",
            # Proxy client default is 1 (keepalive every minute); CCP matches that behaviour
            "expire_time": "{{ raw.expire_time | default(1) }}",
        },
    ),
)

from apollo.integrations.ccp.registry import CcpRegistry  # noqa: E402

CcpRegistry.register("oracle", ORACLE_DEFAULT_CCP)
