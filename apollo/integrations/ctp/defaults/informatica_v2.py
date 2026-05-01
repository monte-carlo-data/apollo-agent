from typing import Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep


class InformaticaV2ClientArgs(TypedDict):
    # Pre-resolved Informatica session, identical to v1's shape — the proxy client
    # is auth-method agnostic and only needs these two values.
    session_id: Required[str]
    api_base_url: Required[str]


# Informatica v2 authenticates via OAuth → JWT → /loginOAuth, using the same
# `OAuthConfiguration` shape monolith stores for Snowflake. v1 (username/password)
# keeps using INFORMATICA_DEFAULT_CTP.
INFORMATICA_V2_DEFAULT_CTP = CtpConfig(
    name="informatica-v2-default",
    steps=[
        # Step 1: OAuth grant against the customer's IDP. Whatever grant_type
        # `raw.oauth` declares (client_credentials, password, etc.) is what the
        # shared `oauth` transform performs. Skipped when the session is already
        # pre-resolved.
        TransformStep(
            type="oauth",
            when="raw.session_id is not defined",
            input={"oauth": "{{ raw.oauth }}"},
            output={"token": "informatica_jwt"},
        ),
        # Step 2: Exchange the JWT at /ma/api/v2/user/loginOAuth for an
        # icSessionId + API base URL. Skipped when pre-resolved.
        TransformStep(
            type="resolve_informatica_session",
            when="raw.session_id is not defined",
            input={
                "jwt_token": "{{ derived.informatica_jwt }}",
                "org_id": "{{ raw.org_id }}",
                "base_url": "{{ raw.base_url | default(none) }}",
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
