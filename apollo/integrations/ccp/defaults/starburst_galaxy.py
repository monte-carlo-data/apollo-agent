from typing import TypedDict, Required

from apollo.integrations.ccp.models import CcpConfig, MapperConfig


class StarburstGalaxyClientArgs(TypedDict):
    host: Required[str]
    port: Required[int]
    user: Required[str]
    password: Required[str]
    http_scheme: Required[str]


STARBURST_GALAXY_DEFAULT_CCP = CcpConfig(
    name="starburst-galaxy-default",
    steps=[],
    mapper=MapperConfig(
        name="starburst_galaxy_client_args",
        schema=StarburstGalaxyClientArgs,
        field_map={
            "host": "{{ raw.host }}",
            "port": "{{ raw.port | int }}",  # DC casts to int: int(port or 443)
            "user": "{{ raw.user }}",
            "password": "{{ raw.password }}",
            "http_scheme": "https",
        },
    ),
)

from apollo.integrations.ccp.registry import CcpRegistry  # noqa: E402

CcpRegistry.register("starburst-galaxy", STARBURST_GALAXY_DEFAULT_CCP)
