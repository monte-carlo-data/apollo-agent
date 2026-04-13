from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig


class DremioClientArgs(TypedDict):
    # Arrow Flight connection
    location: Required[str]  # "grpc://{host}:{port}" or "grpc+tls://{host}:{port}"
    tls_root_certs: NotRequired[bytes]  # PEM-encoded root certificates for TLS
    cert_chain: NotRequired[bytes]  # PEM-encoded client certificate chain
    private_key: NotRequired[bytes]  # PEM-encoded client private key
    override_hostname: NotRequired[str]  # hostname override for TLS cert validation
    # Auth — bearer token sent as authorization header on each request.
    # Not a pyarrow.flight.connect() parameter. Currently read from the credentials
    # top-level by the proxy client; Phase 2 will move it into connect_args and update
    # the proxy client to pop it before passing the remainder to flight.connect().
    # token: NotRequired[str]  — excluded from default field_map until Phase 2


DREMIO_DEFAULT_CTP = CtpConfig(
    name="dremio-default",
    steps=[],
    mapper=MapperConfig(
        name="dremio_client_args",
        schema=DremioClientArgs,
        field_map={
            # grpc+tls when raw.use_tls is set, plain grpc otherwise
            "location": "{{ ('grpc+tls' if raw.use_tls is defined and raw.use_tls else 'grpc') ~ '://' ~ raw.host ~ ':' ~ raw.port }}",
            # token is intentionally omitted here — the proxy client reads it from the
            # credentials top-level today. Phase 2 will add it here and update the proxy client.
        },
    ),
)
