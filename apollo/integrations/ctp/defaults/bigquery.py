from typing import NotRequired, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig
from apollo.integrations.ctp.registry import CtpRegistry


class BqClientArgs(TypedDict):
    # Standard service account JSON fields — all optional because the proxy client
    # falls back to Application Default Credentials when none are provided.
    type: NotRequired[str]  # always "service_account"
    project_id: NotRequired[str]
    private_key_id: NotRequired[str]
    private_key: NotRequired[str]
    client_email: NotRequired[str]
    client_id: NotRequired[str]
    auth_uri: NotRequired[str]
    token_uri: NotRequired[str]
    auth_provider_x509_cert_url: NotRequired[str]
    client_x509_cert_url: NotRequired[str]
    # Popped by the proxy client before passing the rest to from_service_account_info()
    socket_timeout_in_seconds: NotRequired[float]


BIGQUERY_CREDENTIALS_SCHEMA = {
    "connect_args": {
        "type": "dict",
        "required": True,
        # Service-account JSON shape is extensible (Google has added fields
        # over time, e.g. universe_domain). `allow_unknown` keeps forward
        # compatibility while the named fields below cover what the proxy
        # client reads.
        "allow_unknown": True,
        "schema": {
            # `type` is documented as required and "service_account" — code
            # forwards as-is to from_service_account_info(), which itself
            # validates the value. We surface a useful error if it's wrong.
            "type": {"type": "string", "allowed": ["service_account"]},
            "project_id": {"type": "string"},
            "private_key_id": {"type": "string"},
            "private_key": {"type": "string"},
            "client_email": {"type": "string"},
            "client_id": {"type": "string"},
            "auth_uri": {"type": "string"},
            "token_uri": {"type": "string"},
            "auth_provider_x509_cert_url": {"type": "string"},
            "client_x509_cert_url": {"type": "string"},
            # Popped by the proxy client before passing the rest to
            # from_service_account_info().
            "socket_timeout_in_seconds": {"type": "number"},
        },
    },
}

BIGQUERY_DEFAULT_CTP = CtpConfig(
    name="bigquery-default",
    raw_credentials_schema=BIGQUERY_CREDENTIALS_SCHEMA,
    steps=[],
    mapper=MapperConfig(
        name="bq_client_args",
        schema=BqClientArgs,
        field_map={
            # Service account JSON fields — pass through as-is from raw credentials
            "type": "{{ raw.type | default(none) }}",
            "project_id": "{{ raw.project_id | default(none) }}",
            "private_key_id": "{{ raw.private_key_id | default(none) }}",
            "private_key": "{{ raw.private_key | default(none) }}",
            "client_email": "{{ raw.client_email | default(none) }}",
            "client_id": "{{ raw.client_id | default(none) }}",
            "auth_uri": "{{ raw.auth_uri | default(none) }}",
            "token_uri": "{{ raw.token_uri | default(none) }}",
            "auth_provider_x509_cert_url": "{{ raw.auth_provider_x509_cert_url | default(none) }}",
            "client_x509_cert_url": "{{ raw.client_x509_cert_url | default(none) }}",
            # Proxy client pops this before passing remaining fields to
            # Credentials.from_service_account_info()
            "socket_timeout_in_seconds": "{{ raw.socket_timeout_in_seconds | default(none) }}",
        },
    ),
)

CtpRegistry.register("bigquery", BIGQUERY_DEFAULT_CTP)
