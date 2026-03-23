from typing import NotRequired, Required, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig, TransformStep


class SnowflakeClientArgs(TypedDict):
    # Identity — required by the connector for all auth modes
    user: Required[str]
    account: Required[str]
    # Session options
    warehouse: NotRequired[str]
    database: NotRequired[str]
    schema: NotRequired[str]
    role: NotRequired[str]
    login_timeout: NotRequired[int]  # seconds; default 60
    application: NotRequired[str]
    session_parameters: NotRequired[dict]
    # Auth — exactly one of the following groups should be present:
    #   Password:  password
    #   Key-pair:  private_key (DER bytes; loaded from private_key_pem by CCP)
    #   OAuth:     token + authenticator="oauth"
    password: NotRequired[str]
    private_key: NotRequired[bytes]  # DER-encoded; CCP loads from raw.private_key_pem
    token: NotRequired[str]
    authenticator: NotRequired[
        str
    ]  # e.g. "oauth", "snowflake_jwt", or "snowflake" (default)


SNOWFLAKE_DEFAULT_CCP = CcpConfig(
    name="snowflake-default",
    steps=[
        # Key-pair auth: load PEM string → unencrypted DER bytes for the connector.
        # Activate when the user provides private_key_pem; skip for password / OAuth.
        # raw.private_key_pem is consumed here; private_key is never in raw credentials.
        # Optional raw.private_key_passphrase decrypts password-protected PEM keys.
        TransformStep(
            type="load_private_key",
            when="raw.private_key_pem is defined",
            input={
                "pem": "{{ raw.private_key_pem }}",
                "password": "{{ raw.private_key_passphrase | default(none) }}",
            },
            output={"private_key": "private_key_der"},
            field_map={
                "private_key": "{{ derived.private_key_der }}",
            },
        ),
        # OAuth auth: acquire access token from an OAuth 2.0 authorization server.
        # Supports client_credentials and password grant types.
        # raw.oauth is consumed here; token and authenticator are set automatically.
        TransformStep(
            type="oauth",
            when="raw.oauth is defined",
            input={"oauth": "{{ raw.oauth }}"},
            output={"token": "oauth_token"},
            field_map={
                "token": "{{ derived.oauth_token }}",
                "authenticator": "oauth",
            },
        ),
    ],
    mapper=MapperConfig(
        name="snowflake_client_args",
        schema=SnowflakeClientArgs,
        field_map={
            # Required
            "user": "{{ raw.user }}",
            "account": "{{ raw.account }}",
            # Session options
            "warehouse": "{{ raw.warehouse | default(none) }}",
            "database": "{{ raw.database | default(none) }}",
            "schema": "{{ raw.schema | default(none) }}",
            "role": "{{ raw.role | default(none) }}",
            "login_timeout": "{{ raw.login_timeout | default(none) }}",
            "application": "{{ raw.application | default(none) }}",
            "session_parameters": "{{ raw.session_parameters | default(none) }}",
            # Auth fields — omit when absent so the connector selects the auth mode
            # from whichever field is present (password / private_key / token).
            # private_key is NOT here — contributed only by the step field_map above.
            "password": "{{ raw.password | default(none) }}",
            "token": "{{ raw.token | default(none) }}",
            "authenticator": "{{ raw.authenticator | default(none) }}",
        },
    ),
)

from apollo.integrations.ccp.registry import CcpRegistry  # noqa: E402

CcpRegistry.register("snowflake", SNOWFLAKE_DEFAULT_CCP)
