from typing import Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig


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

# Not registered: the proxy client expects connect_args to be the pre-built
# connection string "md:{db_name}?motherduck_token={token}" (a string, not a dict).
# Phase 2 will update MotherDuckProxyClient to build the string from the dict,
# then register here.
