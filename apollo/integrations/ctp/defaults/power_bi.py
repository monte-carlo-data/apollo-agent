from typing import Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep


class PowerBiClientArgs(TypedDict):
    token: Required[str]  # MSAL access token from resolve_msal_token transform
    auth_type: Required[str]  # always "Bearer" for Power BI


POWERBI_DEFAULT_CTP = CtpConfig(
    name="powerbi-default",
    steps=[
        # Resolve MSAL token from raw credentials. Skipped when credentials are
        # pre-shaped (DC path) and a token is already present.
        TransformStep(
            when="raw.auth_mode is defined",
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
            # Passed through when token is already resolved (DC pre-shaped path).
            # Overridden by the resolve_msal_token step field_map when auth_mode is present.
            "token": "{{ raw.token | default(none) }}",
        },
    ),
)

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("power-bi", POWERBI_DEFAULT_CTP)
