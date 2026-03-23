from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig


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


SAP_HANA_DEFAULT_CCP = CcpConfig(
    name="sap-hana-default",
    steps=[],
    mapper=MapperConfig(
        name="sap_hana_client_args",
        schema=SapHanaClientArgs,
        field_map={
            "address": "{{ raw.host }}",
            "port": "{{ raw.port }}",
            "user": "{{ raw.user }}",
            "password": "{{ raw.password }}",
            "databaseName": "{{ raw.db_name | default(none) }}",
            # DC sends login_timeout_in_seconds / query_timeout_in_seconds in seconds;
            # hdbcli expects milliseconds
            "connectTimeout": "{{ raw.login_timeout_in_seconds | int * 1000 if raw.login_timeout_in_seconds is defined else none }}",
            "communicationTimeout": "{{ raw.query_timeout_in_seconds | int * 1000 if raw.query_timeout_in_seconds is defined else none }}",
        },
    ),
)

