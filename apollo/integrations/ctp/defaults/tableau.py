from typing import NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig


class TableauClientArgs(TypedDict):
    server_name: Required[str]
    site_name: NotRequired[str]  # defaults to "" (default site)
    verify_ssl: NotRequired[bool]  # defaults to True
    # DC pre-shaped path: token already resolved by the DC.
    token: NotRequired[str]
    # Flat credentials path: Connected App fields passed through so the proxy client
    # can regenerate a fresh JWT on every sign-in (avoids 5-minute JWT expiry).
    client_id: NotRequired[str]
    secret_id: NotRequired[str]
    secret_value: NotRequired[str]
    username: NotRequired[str]
    token_expiration_seconds: NotRequired[int]


TABLEAU_DEFAULT_CTP = CtpConfig(
    name="tableau-default",
    steps=[],
    mapper=MapperConfig(
        name="tableau_client_args",
        schema=TableauClientArgs,
        field_map={
            "server_name": "{{ raw.server_name }}",
            "site_name": "{{ raw.site_name | default('') }}",
            "verify_ssl": "{{ raw.verify_ssl | default(true) }}",
            # DC pre-shaped path passes token directly; flat path passes Connected App
            # fields so the proxy client can regenerate the JWT per sign-in.
            "token": "{{ raw.token | default(none) }}",
            "client_id": "{{ raw.client_id | default(none) }}",
            "secret_id": "{{ raw.secret_id | default(none) }}",
            "secret_value": "{{ raw.secret_value | default(none) }}",
            "username": "{{ raw.username | default(none) }}",
            "token_expiration_seconds": "{{ raw.token_expiration_seconds | default(none) }}",
        },
    ),
)

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("tableau", TABLEAU_DEFAULT_CTP)
