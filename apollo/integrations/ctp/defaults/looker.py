from typing import NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep


class LookerClientArgs(TypedDict):
    base_url: Required[str]
    client_id: Required[str]
    client_secret: Required[str]
    verify_ssl: NotRequired[bool]  # default True
    ini_file_path: Required[
        str
    ]  # path to temp INI file written by write_ini_file transform


LOOKER_DEFAULT_CTP = CtpConfig(
    name="looker-default",
    steps=[
        TransformStep(
            type="write_ini_file",
            input={
                "section": "Looker",
                "base_url": "{{ raw.base_url }}",
                "client_id": "{{ raw.client_id }}",
                "client_secret": "{{ raw.client_secret }}",
                "verify_ssl": "{{ raw.verify_ssl | default(true) }}",
            },
            output={"path": "looker_ini_path"},
            field_map={"ini_file_path": "{{ derived.looker_ini_path }}"},
        ),
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

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("looker", LOOKER_DEFAULT_CTP)
