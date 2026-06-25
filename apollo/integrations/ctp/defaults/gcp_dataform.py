from typing import Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig
from apollo.integrations.ctp.registry import CtpRegistry


class GcpDataformClientArgs(TypedDict):
    """Shape consumed by GcpDataformProxyClient after CTP transforms."""

    project_id: Required[str]
    service_account_info: Required[dict]
    locations: Required[list[str]]


GCP_DATAFORM_CREDENTIALS_SCHEMA = {
    "connect_args": {
        "type": "dict",
        "required": True,
        "allow_unknown": True,
        "schema": {
            "project_id": {"type": "string", "required": True, "empty": False},
            "service_account_info": {"type": "dict", "required": True},
            "locations": {"type": "list", "required": True, "empty": False},
        },
    },
}

GCP_DATAFORM_DEFAULT_CTP = CtpConfig(
    name="gcp-dataform-default",
    raw_credentials_schema=GCP_DATAFORM_CREDENTIALS_SCHEMA,
    steps=[],
    mapper=MapperConfig(
        name="gcp_dataform_client_args",
        schema=GcpDataformClientArgs,
        field_map={
            "project_id": "{{ raw.project_id }}",
            "service_account_info": "{{ raw.service_account_info }}",
            "locations": "{{ raw.locations }}",
        },
    ),
)

CtpRegistry.register("gcp-dataform", GCP_DATAFORM_DEFAULT_CTP)
