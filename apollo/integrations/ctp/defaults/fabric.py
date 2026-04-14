from typing import NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig


class MsFabricOdbcArgs(TypedDict):
    # Dynamic fields — required; always produced by the default mapper
    SERVER: Required[str]  # "<workspace>.datawarehouse.fabric.microsoft.com,<port>"
    DATABASE: Required[str]
    UID: Required[str]  # "<client_id>@<tenant_id>"
    PWD: Required[str]  # service principal client_secret
    # Static Fabric constants — not required in schema because they come from
    # connect_args_defaults and custom CTP mappers can omit them safely.
    DRIVER: NotRequired[str]
    Authentication: NotRequired[str]
    Encrypt: NotRequired[str]
    TrustServerCertificate: NotRequired[str]


MS_FABRIC_DEFAULT_CTP = CtpConfig(
    name="microsoft-fabric-default",
    # Static ODBC constants that are always correct for Fabric and can be
    # inherited by custom CTPs without having to repeat them.  Mapper output
    # takes precedence, so a custom mapper can override any of these if needed.
    connect_args_defaults={
        "DRIVER": "{ODBC Driver 17 for SQL Server}",
        "Authentication": "ActiveDirectoryServicePrincipal",
        "Encrypt": "yes",
        "TrustServerCertificate": "no",
    },
    steps=[],
    mapper=MapperConfig(
        name="ms_fabric_odbc_args",
        schema=MsFabricOdbcArgs,
        field_map={
            "SERVER": "{{ raw.server | default(raw.host) | default(raw.hostname) }},{{ raw.port | default(1433) }}",
            "DATABASE": "{{ raw.database | default(raw.db_name) }}",
            "UID": "{{ raw.client_id }}@{{ raw.tenant_id }}",
            "PWD": "{{ raw.client_secret }}",
            # Timeout fields — not ODBC params; proxy client pops these before
            # building the connection string.  None values are filtered by the mapper.
            "login_timeout": "{{ raw.login_timeout | default(none) }}",
            "query_timeout_in_seconds": "{{ raw.query_timeout_in_seconds | default(none) }}",
        },
    ),
)


from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("microsoft-fabric", MS_FABRIC_DEFAULT_CTP)
