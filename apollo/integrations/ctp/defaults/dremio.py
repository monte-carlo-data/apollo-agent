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


DREMIO_DEFAULT_CTP = CtpConfig(
    name="dremio-default",
    steps=[],
    mapper=MapperConfig(
        name="dremio_client_args",
        schema=DremioClientArgs,
        field_map={
            # grpc+tls when raw.use_tls is set, plain grpc otherwise
            "location": "{{ ('grpc+tls' if raw.use_tls is defined and raw.use_tls else 'grpc') ~ '://' ~ raw.host ~ ':' ~ raw.port }}",
            "token": "{{ raw.token | default(none) }}",
        },
    ),
)

CtpRegistry.register("dremio", DREMIO_DEFAULT_CTP)
