from typing import NotRequired, Required, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig, TransformStep


class TableauClientArgs(TypedDict):
    server_name: Required[str]
    token: Required[str]  # pre-generated JWT from generate_jwt transform
    site_name: NotRequired[str]  # defaults to "" (default site)
    verify_ssl: NotRequired[bool]  # defaults to True


TABLEAU_DEFAULT_CCP = CcpConfig(
    name="tableau-default",
    steps=[
        # No `when` guard — JWT generation is always required for Tableau Connected Apps.
        TransformStep(
            type="generate_jwt",
            input={
                "username": "{{ raw.username }}",
                "client_id": "{{ raw.client_id }}",
                "secret_id": "{{ raw.secret_id }}",
                "secret_value": "{{ raw.secret_value }}",
                "expiration_seconds": "{{ raw.token_expiration_seconds | default(none) }}",
            },
            output={"token": "tableau_jwt"},
            field_map={"token": "{{ derived.tableau_jwt }}"},
        ),
    ],
    mapper=MapperConfig(
        name="tableau_client_args",
        schema=TableauClientArgs,
        field_map={
            "server_name": "{{ raw.server_name }}",
            "site_name": "{{ raw.site_name | default('') }}",
            "verify_ssl": "{{ raw.verify_ssl | default(true) }}",
        },
    ),
)

# Not registered: the proxy client reads credentials flat (not from connect_args)
# and calls generate_jwt internally on each sign-in.
# Phase 2 will update TableauProxyClient to read from connect_args["token"],
# connect_args["server_name"], etc., then register here.
