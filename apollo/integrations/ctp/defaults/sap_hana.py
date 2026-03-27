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


SAP_HANA_DEFAULT_CTP = CtpConfig(
    name="sap-hana-default",
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
