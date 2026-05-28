from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig


class SapHanaClientArgs(TypedDict):
    # Connection
    address: Required[str]  # hostname
    port: Required[int]
    # Auth
    user: Required[str]
    password: Required[str]
    # Database
    databaseName: NotRequired[str]  # multi-container tenant database name
    # Timeouts (milliseconds)
    connectTimeout: NotRequired[int]
    communicationTimeout: NotRequired[int]
    # SSL
    encrypt: NotRequired[bool]
    sslValidateCertificate: NotRequired[bool]
    sslCryptoProvider: NotRequired[str]
    sslTrustStore: NotRequired[str]  # path to trust store
    sslKeyStore: NotRequired[str]  # path to key store
    sslHostNameInCertificate: NotRequired[str]
    # Connection behaviour
    autocommit: NotRequired[bool]
    reconnect: NotRequired[bool]
    # Performance
    packetSize: NotRequired[int]
    prefetchSize: NotRequired[int]
    compress: NotRequired[bool]


# SAP HANA self-hosted credentials schema mirrors the docs accordion
# (`address`, `databaseName`). CTP accepts `host`/`db_name` for the DC pre-shape
# path but customer self-hosted JSON is expected to follow the docs spellings.
SAP_HANA_CREDENTIALS_SCHEMA = {
    "connect_args": {
        "type": "dict",
        "required": True,
        "schema": {
            "address": {"type": "string", "required": True, "empty": False},
            "port": {"type": "integer", "required": True},
            "user": {"type": "string", "required": True, "empty": False},
            "password": {"type": "string", "required": True, "empty": False},
            "databaseName": {"type": "string", "required": True, "empty": False},
            # Timeout fields per docs Notes (ms-units names; the CTP
            # converts from `*_in_seconds` on the flat path).
            "connectTimeout": {"type": "integer"},
            "communicationTimeout": {"type": "integer"},
        },
    },
}

SAP_HANA_DEFAULT_CTP = CtpConfig(
    name="sap-hana-default",
    raw_credentials_schema=SAP_HANA_CREDENTIALS_SCHEMA,
    steps=[],
    mapper=MapperConfig(
        name="sap_hana_client_args",
        schema=SapHanaClientArgs,
        field_map={
            # DC pre-shapes to connect_args with driver-native name 'address'; flat path uses 'host'
            "address": "{{ raw.host | default(raw.address) }}",
            "port": "{{ raw.port }}",
            "user": "{{ raw.user }}",
            "password": "{{ raw.password }}",
            # DC uses 'databaseName'; flat path uses 'db_name'
            "databaseName": "{{ raw.db_name | default(raw.databaseName) | default(none) }}",
            # Flat path: seconds → milliseconds conversion.
            # DC pre-shapes: already in milliseconds as connectTimeout / communicationTimeout.
            "connectTimeout": "{{ raw.login_timeout_in_seconds | int * 1000 if raw.login_timeout_in_seconds is defined else raw.connectTimeout | default(none) }}",
            "communicationTimeout": "{{ raw.query_timeout_in_seconds | int * 1000 if raw.query_timeout_in_seconds is defined else raw.communicationTimeout | default(none) }}",
        },
    ),
)

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("sap-hana", SAP_HANA_DEFAULT_CTP)
