from typing import Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig
from apollo.integrations.ctp.registry import CtpRegistry


class MotherDuckClientArgs(TypedDict):
    db_name: Required[str]
    token: Required[str]


MOTHERDUCK_DEFAULT_CTP = CtpConfig(
    name="motherduck-default",
    steps=[],
    mapper=MapperConfig(
        name="motherduck_client_args",
        schema=MotherDuckClientArgs,
        field_map={
            "db_name": "{{ raw.db_name }}",
            "token": "{{ raw.token }}",
        },
    ),
)

CtpRegistry.register("motherduck", MOTHERDUCK_DEFAULT_CTP)
