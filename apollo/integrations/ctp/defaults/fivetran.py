from typing import NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep
from apollo.integrations.ctp.registry import CtpRegistry


class FivetranClientArgs(TypedDict):
    # Shape consumed by HttpProxyClient. The DC supplies full Fivetran API URLs
    # to `do_request` directly; the agent only attaches the Basic auth header.
    token: Required[str]
    auth_type: NotRequired[str]  # always "Basic"


# Fivetran self-hosted credentials per docs: api key + api password inside
# connect_args. CTP encodes them to a Basic auth header. The DC pre-shape path
# can also supply a pre-encoded `token`; we accept either form via cerberus
# `anyof` on connect_args field requirements... simpler: just leave both
# field families optional and let the customer follow docs.
FIVETRAN_CREDENTIALS_SCHEMA = {
    "connect_args": {
        "type": "dict",
        "required": True,
        "schema": {
            "fivetran_api_key": {"type": "string", "required": True, "empty": False},
            "fivetran_api_password": {
                "type": "string",
                "required": True,
                "empty": False,
            },
        },
    },
}

FIVETRAN_DEFAULT_CTP = CtpConfig(
    name="fivetran-default",
    raw_credentials_schema=FIVETRAN_CREDENTIALS_SCHEMA,
    steps=[
        # Encode fivetran_api_key:fivetran_api_password into a base64 token.
        # Skipped on the DC pre-shaped path (raw.token already present).
        TransformStep(
            type="encode_basic_auth",
            when="raw.token is not defined",
            input={
                "username": "{{ raw.fivetran_api_key | default(none) }}",
                "password": "{{ raw.fivetran_api_password | default(none) }}",
            },
            output={"token": "fivetran_basic_token"},
        ),
    ],
    mapper=MapperConfig(
        name="fivetran_client_args",
        schema=FivetranClientArgs,
        field_map={
            "token": "{{ derived.fivetran_basic_token | default(raw.token | default(none)) }}",
            "auth_type": "Basic",
        },
    ),
)


CtpRegistry.register("fivetran", FIVETRAN_DEFAULT_CTP)
