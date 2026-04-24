from typing import NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep


class InformaticaClientArgs(TypedDict):
    # Pre-resolved session produced by the resolve_informatica_session transform.
    # The proxy client is auth-method agnostic — it only needs these two values.
    session_id: Required[str]
    api_base_url: Required[str]


INFORMATICA_DEFAULT_CTP = CtpConfig(
    name="informatica-default",
    steps=[
        # Skipped when session_id is already present (DC pre-shaped path or custom CTP
        # config that resolved the session upstream). When running, supports both
        # username/password (V2 or V3) and JWT loginOAuth modes.
        TransformStep(
            type="resolve_informatica_session",
            when="raw.session_id is not defined",
            input={
                "username": "{{ raw.username | default(none) }}",
                "password": "{{ raw.password | default(none) }}",
                "informatica_auth": "{{ raw.informatica_auth | default(none) }}",
                "base_url": "{{ raw.base_url | default(none) }}",
                "jwt_token": "{{ raw.jwt_token | default(none) }}",
                "org_id": "{{ raw.org_id | default(none) }}",
            },
            output={
                "session_id": "informatica_session_id",
                "api_base_url": "informatica_api_base_url",
            },
        )
    ],
    mapper=MapperConfig(
        name="informatica_client_args",
        schema=InformaticaClientArgs,
        field_map={
            # When the step ran: read from derived. When pre-resolved: fall back to raw.
            "session_id": "{{ derived.informatica_session_id | default(raw.session_id) }}",
            "api_base_url": "{{ derived.informatica_api_base_url | default(raw.api_base_url) }}",
        },
    ),
)


from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("informatica", INFORMATICA_DEFAULT_CTP)
