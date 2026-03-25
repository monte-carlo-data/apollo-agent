from typing import NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep


class Db2ClientArgs(TypedDict):
    # Connection identity — DB2 ODBC key names are uppercase by convention
    HOSTNAME: Required[str]
    PORT: Required[int]
    DATABASE: Required[str]
    UID: Required[str]
    PWD: Required[str]
    PROTOCOL: Required[str]  # always "TCPIP"
    # Optional timeouts (ibm_db connection string parameters)
    querytimeout: NotRequired[int]
    connecttimeout: NotRequired[int]
    # SSL — contributed only by the ssl transform step when ca_data is present
    Security: NotRequired[str]  # "SSL"
    SSLServerCertificate: NotRequired[str]  # path to CA PEM file


DB2_DEFAULT_CTP = CtpConfig(
    name="db2-default",
    steps=[
        # SSL: write CA cert to a temp file and add Security + SSLServerCertificate
        # to the connect_args dict. The proxy client serializes the full dict to the
        # ODBC connection string, so these fields arrive at ibm_db.connect() naturally.
        #
        # On the legacy path the proxy client reads ssl_options from the top level of
        # credentials and handles SSL itself; this step only fires on the CTP path
        # (when the DC sends flat credentials without a pre-built connect_args dict).
        TransformStep(
            type="tmp_file_write",
            when=(
                "raw.ssl_options is defined"
                " and raw.ssl_options.ca_data is defined"
                " and not (raw.ssl_options.disabled is defined and raw.ssl_options.disabled)"
            ),
            input={
                "contents": "{{ raw.ssl_options.ca_data }}",
                "file_suffix": ".pem",
                "mode": "0600",
            },
            output={"path": "ssl_ca_path"},
            field_map={
                "Security": "SSL",
                "SSLServerCertificate": "{{ derived.ssl_ca_path }}",
            },
        ),
    ],
    mapper=MapperConfig(
        name="db2_client_args",
        schema=Db2ClientArgs,
        field_map={
            "HOSTNAME": "{{ raw.host }}",
            "PORT": "{{ raw.port | default(50000) }}",
            "DATABASE": "{{ raw.db_name | default(raw.database) }}",
            "UID": "{{ raw.user | default(raw.username) }}",
            "PWD": "{{ raw.password }}",
            "PROTOCOL": "TCPIP",
            "querytimeout": "{{ raw.query_timeout_in_seconds | default(none) }}",
            "connecttimeout": "{{ raw.connect_timeout | default(none) }}",
        },
    ),
)
