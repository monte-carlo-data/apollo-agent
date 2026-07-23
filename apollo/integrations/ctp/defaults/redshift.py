from typing import NotRequired, Required, TypedDict

from apollo.credentials.schema.common import SSL_OPTIONS_FIELD
from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep


class RedshiftClientArgs(TypedDict):
    # Required connection identifiers
    host: Required[str]
    port: Required[int]
    dbname: Required[str]
    user: Required[str]
    # Present for password auth. For IAM-federated auth the secret carries no
    # static password — it's supplied at connect time by the auth-rule's
    # resolve_redshift_credentials step (minted via GetClusterCredentials) and
    # merged into these client args before the connector runs. See SUP-532.
    password: Required[str]
    # SSL
    sslmode: NotRequired[str]
    sslrootcert: NotRequired[str]
    sslcert: NotRequired[str]
    sslkey: NotRequired[str]
    sslcrl: NotRequired[str]
    # Connection behavior
    connect_timeout: NotRequired[int]
    application_name: NotRequired[str]
    options: NotRequired[str]
    # TCP keepalives
    keepalives: NotRequired[int]
    keepalives_idle: NotRequired[int]
    keepalives_interval: NotRequired[int]
    keepalives_count: NotRequired[int]


# Connection fields shared by both Redshift auth modes. Spread into each
# ``oneof_schema`` variant below so each variant is a complete,
# independently-valid schema (a requirement for ``oneof_schema``).
#
# db-name: the mapper reads ``db_name`` first, falling back to ``dbname`` /
# ``database`` (see mapper field_map below). All three are accepted as optional
# aliases so a secret authored with any spelling passes; a genuinely missing
# database name surfaces at connect time rather than as a schema error (avoids
# rejecting existing ``dbname``-keyed secrets while accepting the documented
# ``db_name``). ``port`` is documented as a string ("5439") but the connector
# accepts both string and integer; CTP defaults to 5439.
_REDSHIFT_COMMON_CONNECT_ARGS = {
    "host": {"type": "string", "required": True, "empty": False},
    "db_name": {"type": "string"},
    "dbname": {"type": "string"},
    "database": {"type": "string"},
    "user": {"type": "string"},  # CTP defaults to "awsuser"
    "port": {"type": ["string", "integer"]},
    "connect_timeout": {"type": "integer"},
    "query_timeout_in_seconds": {"type": "integer"},
    "ssl_mode": {"type": "string"},
}

# Redshift self-hosted credentials schema. Two auth modes, expressed as
# ``oneof_schema`` variants (see snowflake.py for the canonical pattern):
#   1. Monte Carlo-managed / password auth: username + static password.
#   2. IAM-federated auth (Connection Auth Rules): the agent mints temporary
#      credentials via GetClusterCredentials, so there is NO static password —
#      the secret carries cluster_identifier / db_user / aws_region instead
#      (consumed by the resolve_redshift_credentials transform in the custom
#      CTP config). Requiring `password` here previously forced federated
#      customers to add a dummy password. See SUP-532.
# Cerberus picks the matching variant; zero matches (no auth field) or more
# than one (ambiguous) fails with a diagnostic listing every candidate.
REDSHIFT_CREDENTIALS_SCHEMA = {
    "connect_args": {
        "type": "dict",
        "required": True,
        "oneof_schema": [
            # Password auth.
            {
                **_REDSHIFT_COMMON_CONNECT_ARGS,
                "password": {"type": "string", "required": True, "empty": False},
            },
            # IAM-federated auth (GetClusterCredentials). No static password;
            # temporary DbUser/DbPassword are resolved at connect time.
            {
                **_REDSHIFT_COMMON_CONNECT_ARGS,
                "cluster_identifier": {
                    "type": "string",
                    "required": True,
                    "empty": False,
                },
                "db_user": {"type": "string", "required": True, "empty": False},
                "aws_region": {"type": "string", "required": True, "empty": False},
                "assumable_role": {"type": "string"},
                "external_id": {"type": "string"},
                "duration_seconds": {"type": ["string", "integer"]},
            },
        ],
    },
    "ssl_options": SSL_OPTIONS_FIELD,
    # Top-level autocommit per docs example.
    "autocommit": {"type": "boolean"},
}

REDSHIFT_DEFAULT_CTP = CtpConfig(
    name="redshift-default",
    raw_credentials_schema=REDSHIFT_CREDENTIALS_SCHEMA,
    steps=[
        TransformStep(
            type="resolve_ssl_options",
            when="raw.ssl_options is mapping",
            input={"ssl_options": "{{ raw.ssl_options }}"},
            output={
                "ssl_options": "ssl_options",
                "ca_path": "ssl_ca_path",
            },
            field_map={
                "sslrootcert": "{{ derived.ssl_ca_path if derived.ssl_ca_path is defined else none }}",
                "sslmode": "{{ raw.ssl_mode | default('require') if derived.ssl_ca_path is defined else raw.ssl_mode | default(none) }}",
            },
        )
    ],
    mapper=MapperConfig(
        name="redshift_client_args",
        schema=RedshiftClientArgs,
        field_map={
            "host": "{{ raw.host }}",
            "port": "{{ raw.port | default(5439) }}",
            "dbname": "{{ raw.db_name | default(raw.dbname) | default(raw.database) }}",
            "user": "{{ raw.user | default('awsuser') }}",
            "password": "{{ raw.password }}",
            "connect_timeout": "{{ raw.connect_timeout | default(none) }}",
            # statement_timeout in ms; derived from query_timeout_in_seconds when provided
            "options": "{{ '-c statement_timeout=' ~ (raw.query_timeout_in_seconds | int * 1000) if raw.query_timeout_in_seconds is defined else none }}",
            "sslmode": "{{ raw.ssl_mode | default(none) }}",
        },
    ),
    # TCP keepalives required for AWS PrivateLink; injected as defaults so custom
    # CTP configs inherit them without having to redeclare them.
    connect_args_defaults={
        "connect_timeout": 30,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    },
)

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("redshift", REDSHIFT_DEFAULT_CTP)
