from typing import Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep


class PowerBiClientArgs(TypedDict):
    token: Required[str]  # MSAL access token from resolve_msal_token transform
    auth_type: Required[str]  # always "Bearer" for Power BI


POWERBI_DEFAULT_CTP = CtpConfig(
    name="powerbi-default",
    steps=[
        TransformStep(
            type="resolve_msal_token",
            input={
                "auth_mode": "{{ raw.auth_mode }}",
                "client_id": "{{ raw.client_id }}",
                "tenant_id": "{{ raw.tenant_id }}",
                "client_secret": "{{ raw.client_secret | default(none) }}",
                "username": "{{ raw.username | default(none) }}",
                "password": "{{ raw.password | default(none) }}",
            },
            output={"token": "msal_token"},
            field_map={"token": "{{ derived.msal_token }}"},
        ),
    ],
    mapper=MapperConfig(
        name="powerbi_client_args",
        schema=PowerBiClientArgs,
        field_map={
            "auth_type": "Bearer",
        },
    ),
)

# Not registered: the proxy client reads credentials flat and calls MSAL internally,
# then forwards token + auth_type="Bearer" to HttpProxyClient.
# Phase 2 will update PowerBiProxyClient to read from connect_args["token"] directly,
# then register here.
