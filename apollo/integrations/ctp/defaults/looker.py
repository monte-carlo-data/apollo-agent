from typing import NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig


class LookerClientArgs(TypedDict):
    base_url: Required[str]
    client_id: Required[str]
    client_secret: Required[str]
    verify_ssl: NotRequired[bool]  # default True


LOOKER_DEFAULT_CTP = CtpConfig(
    name="looker-default",
    steps=[
        # Phase 2: add a write_ini_file transform that materialises these fields
        # into a temp [Looker] INI file and stores the path in state.derived.
        # The proxy client would then call init40(connect_args["ini_file_path"])
        # instead of writing the INI itself.
    ],
    mapper=MapperConfig(
        name="looker_client_args",
        schema=LookerClientArgs,
        field_map={
            "base_url": "{{ raw.base_url }}",
            "client_id": "{{ raw.client_id }}",
            "client_secret": "{{ raw.client_secret }}",
            "verify_ssl": "{{ raw.verify_ssl | default(true) }}",
        },
    ),
)

# Not registered: the proxy client reads credentials flat and writes an INI file
# by iterating credentials.keys(). After CTP the output is {"connect_args": {...}},
# which would write connect_args=<dict> into the INI — wrong.
# Phase 2 will add a write_ini_file transform and update the proxy client to accept
# the file path from connect_args, then register here.
