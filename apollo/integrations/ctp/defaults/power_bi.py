from typing import Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig


class PowerBiClientArgs(TypedDict, total=False):
    auth_type: Required[str]  # always "Bearer" for Power BI
    # Raw-creds mode: the agent mints a token per scope, selected by request host
    # (api.powerbi.com -> Power BI scope, api.fabric.microsoft.com -> Fabric scope). Both
    # audiences come from the same credentials — see PowerBiTokenProvider.
    auth_mode: str  # "service_principal" or "primary_user"
    client_id: str
    tenant_id: str
    client_secret: str
    username: str
    password: str
    # Legacy pre-shaped mode: a token already minted by the data-collector (Power BI-scoped,
    # rare for self-hosted). Served only for api.powerbi.com since there are no creds to re-mint.
    token: str


# Power BI self-hosted creds are flat top-level (no `connect_args` wrapper) per docs. The agent
# owns token acquisition (the DC sends raw creds and injects no Authorization header in agent
# mode), so the CTP passes the raw MSAL params through to `connect_args` unchanged and the proxy
# client mints the correctly-scoped token per request host. `auth_mode` is `service_principal`
# (client-credentials) or `primary_user` (username/password).
POWER_BI_CREDENTIALS_SCHEMA = {
    "auth_mode": {"type": "string", "allowed": ["service_principal", "primary_user"]},
    "client_id": {"type": "string"},
    "tenant_id": {"type": "string"},
    "client_secret": {"type": "string"},
    "username": {"type": "string"},
    "password": {"type": "string"},
    # Pre-resolved MSAL token (DC pre-shape path, rare for self-hosted).
    "token": {"type": "string"},
}

POWERBI_DEFAULT_CTP = CtpConfig(
    name="powerbi-default",
    raw_credentials_schema=POWER_BI_CREDENTIALS_SCHEMA,
    steps=[],
    mapper=MapperConfig(
        name="powerbi_client_args",
        schema=PowerBiClientArgs,
        # Pass the raw MSAL params (or a legacy pre-shaped token) straight through. Token
        # acquisition happens per request in PowerBiProxyClient / PowerBiTokenProvider, which
        # needs the raw credentials to mint a Power BI- or Fabric-scoped token by host. Absent
        # fields resolve to None and are dropped by the mapper.
        field_map={
            "auth_type": "Bearer",
            "auth_mode": "{{ raw.auth_mode | default(none) }}",
            "client_id": "{{ raw.client_id | default(none) }}",
            "tenant_id": "{{ raw.tenant_id | default(none) }}",
            "client_secret": "{{ raw.client_secret | default(none) }}",
            "username": "{{ raw.username | default(none) }}",
            "password": "{{ raw.password | default(none) }}",
            "token": "{{ raw.token | default(none) }}",
        },
    ),
)

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("power-bi", POWERBI_DEFAULT_CTP)
