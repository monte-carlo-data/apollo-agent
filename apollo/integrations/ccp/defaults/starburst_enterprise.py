from typing import TypedDict, Required, NotRequired

from apollo.integrations.ccp.models import CcpConfig, MapperConfig


class StarburstEnterpriseClientArgs(TypedDict):
    host: Required[str]
    port: Required[int]
    user: Required[str]
    password: Required[str]
    http_scheme: Required[str]
    ssl_options: NotRequired[dict]


STARBURST_ENTERPRISE_DEFAULT_CCP = CcpConfig(
    name="starburst-enterprise-default",
    steps=[],
    mapper=MapperConfig(
        name="starburst_enterprise_client_args",
        schema=StarburstEnterpriseClientArgs,
        field_map={
            "host": "{{ raw.host }}",
            "port": "{{ raw.port | int }}",  # DC input has port as string e.g. "8443"
            "user": "{{ raw.user }}",
            "password": "{{ raw.password }}",
            "http_scheme": "https",
            "ssl_options": "{{ raw.ssl_options | default(none) }}",
        },
    ),
)

from apollo.integrations.ccp.registry import CcpRegistry  # noqa: E402

CcpRegistry.register("starburst-enterprise", STARBURST_ENTERPRISE_DEFAULT_CCP)
