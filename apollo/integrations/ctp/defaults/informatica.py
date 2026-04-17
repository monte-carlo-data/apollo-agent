from typing import NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig


class InformaticaClientArgs(TypedDict):
    username: Required[str]
    password: Required[str]
    # V2 or V3 auth. Defaults to "v3" inside InformaticaProxyClient when absent.
    informatica_auth: NotRequired[str]
    # Login base URL. Defaults to https://dm-us.informaticacloud.com when absent.
    base_url: NotRequired[str]


INFORMATICA_DEFAULT_CTP = CtpConfig(
    name="informatica-default",
    steps=[],
    mapper=MapperConfig(
        name="informatica_client_args",
        schema=InformaticaClientArgs,
        field_map={
            "username": "{{ raw.username }}",
            "password": "{{ raw.password }}",
            "informatica_auth": "{{ raw.informatica_auth | default(none) }}",
            "base_url": "{{ raw.base_url | default(none) }}",
        },
    ),
)


from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("informatica", INFORMATICA_DEFAULT_CTP)
