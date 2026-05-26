from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig
from apollo.integrations.ctp.registry import CtpRegistry


class DremioClientArgs(TypedDict):
    # Arrow Flight connection
    location: Required[str]  # "grpc://{host}:{port}" or "grpc+tls://{host}:{port}"
    tls_root_certs: NotRequired[bytes]  # PEM-encoded root certificates for TLS
    cert_chain: NotRequired[bytes]  # PEM-encoded client certificate chain
    private_key: NotRequired[bytes]  # PEM-encoded client private key
    override_hostname: NotRequired[str]  # hostname override for TLS cert validation
    # Auth — bearer token popped by the proxy client before passing remainder to flight.connect
    token: NotRequired[str]


DREMIO_CREDENTIALS_SCHEMA = {
    "connect_args": {
        "type": "dict",
        "required": True,
        "schema": {
            # Pre-built Flight location string ("grpc[+tls]://host:port").
            # Docs only show this form; CTP also accepts host/port/use_tls
            # but that path is not customer-facing.
            "location": {"type": "string", "required": True, "empty": False},
        },
    },
    # Token is top-level per docs (NOT inside connect_args).
    "token": {"type": "string", "required": True, "empty": False},
}

DREMIO_DEFAULT_CTP = CtpConfig(
    name="dremio-default",
    raw_credentials_schema=DREMIO_CREDENTIALS_SCHEMA,
    steps=[],
    mapper=MapperConfig(
        name="dremio_client_args",
        schema=DremioClientArgs,
        field_map={
            # Pass through a pre-built location string (DC pre-shaped path), or
            # construct from host/port/use_tls (flat credentials path).
            "location": "{{ raw.location if raw.location is defined else (('grpc+tls' if raw.use_tls is defined and raw.use_tls else 'grpc') ~ '://' ~ raw.host ~ ':' ~ raw.port) }}",
            "token": "{{ raw.token | default(none) }}",
        },
    ),
)

CtpRegistry.register("dremio", DREMIO_DEFAULT_CTP)
