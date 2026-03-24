from typing import NotRequired, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig


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


BIGQUERY_DEFAULT_CCP = CcpConfig(
    name="bigquery-default",
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

