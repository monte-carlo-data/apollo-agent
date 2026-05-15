from typing import NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep
from apollo.integrations.ctp.registry import CtpRegistry


class FivetranClientArgs(TypedDict):
    # Shape consumed by HttpProxyClient. The DC supplies full Fivetran API URLs
    # to `do_request` directly; the agent only attaches the Basic auth header.
    token: Required[str]
    auth_type: NotRequired[str]  # always "Basic"


FIVETRAN_DEFAULT_CTP = CtpConfig(
    name="fivetran-default",
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
