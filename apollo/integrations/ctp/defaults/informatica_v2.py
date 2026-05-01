from typing import Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep


class InformaticaV2ClientArgs(TypedDict):
    # Pre-resolved Informatica session, identical to v1's shape — the proxy client
    # is auth-method agnostic and only needs these two values.
    session_id: Required[str]
    api_base_url: Required[str]


# v2 supports two auth modes, discriminated by `raw.auth_mode`:
#   - "oauth"    → OAuth grant against the customer's IDP → JWT → /loginOAuth
#   - "password" → V2 or V3 password login (same as v1's INFORMATICA_DEFAULT_CTP)
# `resolve_informatica_session` picks the right Informatica login path from
# whichever fields are present. We only need a leading OAuth step in oauth mode.
INFORMATICA_V2_DEFAULT_CTP = CtpConfig(
    name="informatica-v2-default",
    steps=[
        # Step 1 (OAuth mode only): OAuth grant against the customer's IDP using
        # the shared `oauth` transform. Whatever grant_type `raw.oauth` declares
        # (client_credentials, password, etc.) is what the transform performs.
        TransformStep(
            type="oauth",
            when="raw.session_id is not defined and raw.auth_mode == 'oauth'",
            input={"oauth": "{{ raw.oauth }}"},
            output={"token": "informatica_jwt"},
        ),
        # Step 2: Resolve the Informatica session. In oauth mode, exchange the
        # JWT from step 1 at /loginOAuth. In password mode, do a V2 or V3 login.
        TransformStep(
            type="resolve_informatica_session",
            when="raw.session_id is not defined",
            input={
                "username": "{{ raw.username | default(none) }}",
                "password": "{{ raw.password | default(none) }}",
                "informatica_auth": "{{ raw.informatica_auth | default(none) }}",
                "base_url": "{{ raw.base_url | default(none) }}",
                "jwt_token": "{{ derived.informatica_jwt | default(none) }}",
                "org_id": "{{ raw.org_id | default(none) }}",
            },
            output={
                "session_id": "informatica_session_id",
                "api_base_url": "informatica_api_base_url",
            },
        ),
    ],
    mapper=MapperConfig(
        name="informatica_v2_client_args",
        schema=InformaticaV2ClientArgs,
        field_map={
            # When the steps ran: read from derived. When pre-resolved: fall
            # back to raw (the DC pre-shaped path).
            "session_id": "{{ derived.informatica_session_id | default(raw.session_id) }}",
            "api_base_url": "{{ derived.informatica_api_base_url | default(raw.api_base_url) }}",
        },
    ),
)


from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("informatica-v2", INFORMATICA_V2_DEFAULT_CTP)
