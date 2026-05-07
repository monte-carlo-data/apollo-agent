from typing import NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep
from apollo.integrations.ctp.registry import CtpRegistry


class MulesoftClientArgs(TypedDict):
    # Shape consumed by HttpProxyClient (extended in Phase 3): `token` + `auth_type`
    # build the bearer header in do_request / download_bytes; `api_base_url` is read
    # by the new do_request_relative method to prepend to caller-supplied paths;
    # `ssl_verify` flows to HttpProxyClient._ssl_verify for both methods.
    token: Required[str]
    auth_type: Required[str]
    api_base_url: Required[str]
    ssl_verify: NotRequired[bool | str]


MULESOFT_DEFAULT_CTP = CtpConfig(
    name="mulesoft-default",
    steps=[
        # Step 1 — Resolve auth/api URLs from region (with override validation)
        # and build the OAuth config dict for the next step. Skipped on the DC
        # pre-shaped path (raw.token already present).
        TransformStep(
            type="resolve_mulesoft_endpoints",
            when="raw.token is not defined",
            input={
                "client_id": "{{ raw.client_id | default(none) }}",
                "client_secret": "{{ raw.client_secret | default(none) }}",
                "region": "{{ raw.region | default(none) }}",
                "auth_url": "{{ raw.auth_url | default(none) }}",
                "api_base_url": "{{ raw.api_base_url | default(none) }}",
            },
            output={
                "oauth_config": "mulesoft_oauth_config",
                "api_base_url": "mulesoft_api_base_url",
            },
        ),
        # Step 2 — OAuth client_credentials grant. Reuses the generic shared
        # transform; the oauth_config dict was prepared by step 1.
        TransformStep(
            type="oauth",
            when="raw.token is not defined",
            input={"oauth": "{{ derived.mulesoft_oauth_config }}"},
            output={"token": "mulesoft_token"},
        ),
    ],
    mapper=MapperConfig(
        name="mulesoft_client_args",
        schema=MulesoftClientArgs,
        # Nested `default(none)` on the inner reference is load-bearing: it turns
        # a missing pre-shaped field into None (which the mapper drops, then flags
        # as a missing required field) instead of letting Jinja raise UndefinedError.
        field_map={
            "token": "{{ derived.mulesoft_token | default(raw.token | default(none)) }}",
            "auth_type": "Bearer",
            "api_base_url": "{{ derived.mulesoft_api_base_url | default(raw.api_base_url | default(none)) }}",
            "ssl_verify": "{{ raw.ssl_verify | default(none) }}",
        },
    ),
)


CtpRegistry.register("mulesoft", MULESOFT_DEFAULT_CTP)
