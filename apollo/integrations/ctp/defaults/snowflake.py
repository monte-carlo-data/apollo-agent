from typing import NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep
from apollo.integrations.ctp.registry import CtpRegistry


# Common Snowflake identity + session fields shared by every auth mode.
# Spread into each ``oneof_schema`` variant below so each variant is a
# complete, independently-valid schema (a requirement for ``oneof_schema``).
_SNOWFLAKE_COMMON_CONNECT_ARGS = {
    "user": {"type": "string", "required": True, "empty": False},
    "account": {"type": "string", "required": True, "empty": False},
    "warehouse": {"type": "string"},
    "database": {"type": "string"},
    "schema": {"type": "string"},
    "role": {"type": "string"},
    "login_timeout": {"type": "integer"},
    "application": {"type": "string"},
    "session_parameters": {"type": "dict"},
    "authenticator": {"type": "string"},
}


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
    #   Key-pair:  private_key (DER bytes; loaded from private_key_pem by CTP)
    #   OAuth:     token + authenticator="oauth"
    password: NotRequired[str]
    private_key: NotRequired[bytes]  # DER-encoded; CTP loads from raw.private_key_pem
    token: NotRequired[str]
    authenticator: NotRequired[
        str
    ]  # e.g. "oauth", "snowflake_jwt", or "snowflake" (default)


# Each variant under ``oneof_schema`` is a complete, independently-valid
# shape for the connect_args dict. Cerberus picks the matching variant; if
# zero match (no auth field present) or more than one matches (ambiguous,
# e.g. both password AND private_key_pem set), validation fails with a
# diagnostic listing every candidate variant.
SNOWFLAKE_CREDENTIALS_SCHEMA = {
    "connect_args": {
        "type": "dict",
        "required": True,
        "oneof_schema": [
            # Password auth.
            {
                **_SNOWFLAKE_COMMON_CONNECT_ARGS,
                "password": {"type": "string", "required": True, "empty": False},
            },
            # Key-pair auth — PEM form (flat-credentials path; CTP decodes to DER).
            {
                **_SNOWFLAKE_COMMON_CONNECT_ARGS,
                "private_key_pem": {"type": "string", "required": True, "empty": False},
                "private_key_passphrase": {"type": "string"},
            },
            # Key-pair auth — PKCS#8-base64 form (DC pre-shape path, what the
            # public docs document today).
            {
                **_SNOWFLAKE_COMMON_CONNECT_ARGS,
                "private_key": {"type": "string", "required": True, "empty": False},
            },
            # OAuth — customer-supplied OAuth grant config.
            {
                **_SNOWFLAKE_COMMON_CONNECT_ARGS,
                "oauth": {
                    "type": "dict",
                    "required": True,
                    "allow_unknown": True,
                },
            },
            # Pre-resolved bearer token (rare; pair with authenticator="oauth").
            {
                **_SNOWFLAKE_COMMON_CONNECT_ARGS,
                "token": {"type": "string", "required": True, "empty": False},
            },
        ],
    },
}

SNOWFLAKE_DEFAULT_CTP = CtpConfig(
    name="snowflake-default",
    raw_credentials_schema=SNOWFLAKE_CREDENTIALS_SCHEMA,
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
            "application": "{{ raw.application | default(none) }}",  # overrides connect_args_defaults when set
            "session_parameters": "{{ raw.session_parameters | default(none) }}",
            # Auth fields — omit when absent so the connector selects the auth mode
            # from whichever field is present (password / private_key / token).
            # private_key is also accepted here for the DC-pre-shaped path where bytes
            # have already been decoded; the load_private_key step overrides it when
            # raw.private_key_pem is present.
            "password": "{{ raw.password | default(none) }}",
            "private_key": "{{ raw.private_key | default(none) }}",
            "token": "{{ raw.token | default(none) }}",
            "authenticator": "{{ raw.authenticator | default(none) }}",
        },
    ),
    # Partner application name for Snowflake's usage tracking; always "Monte Carlo"
    # unless overridden explicitly via raw.application in a custom CTP config.
    connect_args_defaults={"application": "Monte Carlo"},
)

CtpRegistry.register("snowflake", SNOWFLAKE_DEFAULT_CTP)
