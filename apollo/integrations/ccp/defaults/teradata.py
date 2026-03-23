from typing import NotRequired, Required, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig, TransformStep


class TeradataClientArgs(TypedDict):
    # Connection identity
    host: Required[str]
    user: Required[str]
    password: Required[str]
    # Port — exactly one of these will be present:
    #   dbs_port  for plain (non-SSL) connections  — teradatasql default 1025
    #   https_port for SSL connections (replaces dbs_port; typically 443)
    dbs_port: NotRequired[int]
    https_port: NotRequired[int]
    # Timeouts
    request_timeout: NotRequired[int]  # query timeout in seconds
    logon_timeout: NotRequired[int]  # login timeout in seconds
    # Protocol options
    tmode: NotRequired[str]  # transaction mode; default "TERA"
    sslmode: NotRequired[str]  # SSL mode; default "PREFER"
    logmech: NotRequired[str]  # logon mechanism; default "TD2"
    # SSL — contributed only by the ssl transform step when ca_data is present
    encryptdata: NotRequired[str]  # "true" (quoted boolean per teradatasql docs)
    sslca: NotRequired[str]  # path to CA bundle PEM file


TERADATA_DEFAULT_CCP = CcpConfig(
    name="teradata-default",
    steps=[
        # SSL: write CA cert to a temp file and switch from dbs_port to https_port.
        #
        # When this step runs it contributes:
        #   sslca       — path to the written PEM file
        #   encryptdata — "true" (string, required by teradatasql)
        #   https_port  — the port value, under the SSL-specific key name
        #   dbs_port    — none, which overrides the mapper's dbs_port and removes it
        #
        # When disabled=True is set alongside ca_data the step is skipped, so
        # dbs_port is emitted as normal and no SSL fields are added.
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
                "sslca": "{{ derived.ssl_ca_path }}",
                "encryptdata": "true",
                "https_port": "{{ raw.port }}",
                "dbs_port": "{{ none }}",  # suppress mapper's dbs_port
            },
        ),
    ],
    mapper=MapperConfig(
        name="teradata_client_args",
        schema=TeradataClientArgs,
        field_map={
            "host": "{{ raw.host }}",
            "user": "{{ raw.user }}",
            "password": "{{ raw.password }}",
            # Plain-connection port; overridden to none by the SSL step when active
            "dbs_port": "{{ raw.port | default(none) }}",
            "request_timeout": "{{ raw.query_timeout_in_seconds | default(none) }}",
            "logon_timeout": "{{ raw.login_timeout_in_seconds | default(none) }}",
            # Protocol options — default values match DC's TeradataConnectionSettingsSchema
            "tmode": "{{ raw.tmode | default('TERA') }}",
            "sslmode": "{{ raw.sslmode | default('PREFER') }}",
            "logmech": "{{ raw.logmech | default('TD2') }}",
        },
    ),
)

from apollo.integrations.ccp.registry import CcpRegistry  # noqa: E402

CcpRegistry.register("teradata", TERADATA_DEFAULT_CCP)
