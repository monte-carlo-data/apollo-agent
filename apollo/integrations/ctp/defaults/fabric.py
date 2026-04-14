from typing import NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig


class MsFabricOdbcArgs(TypedDict):
    # Driver and server
    DRIVER: Required[str]
    SERVER: Required[
        str
    ]  # Fabric SQL Analytics Endpoint, e.g. "<workspace>.datawarehouse.fabric.microsoft.com,1433"
    DATABASE: Required[str]
    # Azure AD service principal authentication
    Authentication: Required[str]  # "ActiveDirectoryServicePrincipal"
    UID: Required[str]  # "<client_id>@<tenant_id>" — ODBC format for service principal
    PWD: Required[str]  # service principal client_secret
    # Encryption — always enforced for Fabric
    Encrypt: NotRequired[str]  # "yes"
    TrustServerCertificate: NotRequired[str]  # "no"


MS_FABRIC_DEFAULT_CTP = CtpConfig(
    name="microsoft-fabric-default",
    steps=[],
    mapper=MapperConfig(
        name="ms_fabric_odbc_args",
        schema=MsFabricOdbcArgs,
        field_map={
            "DRIVER": "{ODBC Driver 17 for SQL Server}",
            "SERVER": "{{ raw.server | default(raw.host) | default(raw.hostname) }},{{ raw.port | default(1433) }}",
            "DATABASE": "{{ raw.database | default(raw.db_name) }}",
            "Authentication": "ActiveDirectoryServicePrincipal",
            "UID": "{{ raw.client_id }}@{{ raw.tenant_id }}",
            "PWD": "{{ raw.client_secret }}",
            "Encrypt": "yes",
            "TrustServerCertificate": "no",
            # Timeout fields — not ODBC params; proxy client pops these before building the connection string
            "login_timeout": "{{ raw.login_timeout | default(none) }}",
            "query_timeout_in_seconds": "{{ raw.query_timeout_in_seconds | default(none) }}",
        },
    ),
)


from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

# CtpRegistry.register("microsoft-fabric", MS_FABRIC_DEFAULT_CTP)
