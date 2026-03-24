from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig, TransformStep


class DatabricksSqlClientArgs(TypedDict):
    server_hostname: Required[str]
    http_path: Required[str]
    access_token: NotRequired[str]  # PAT auth
    credentials_provider: NotRequired[
        Any
    ]  # OAuth callable from resolve_databricks_oauth
    _use_arrow_native_complex_types: NotRequired[bool]
    _user_agent_entry: NotRequired[str]


DATABRICKS_DEFAULT_CCP = CcpConfig(
    name="databricks-default",
    steps=[
        # OAuth path: build credentials_provider callable and contribute it to connect_args.
        # databricks_client_id / databricks_client_secret are intentionally excluded from the
        # mapper field_map so they are not passed to sql.connect, which means the proxy
        # client's own _credentials_use_oauth check is False and it will not overwrite the
        # CCP-provided callable.
        TransformStep(
            type="resolve_databricks_oauth",
            when=(
                "raw.databricks_client_id is defined"
                " and raw.databricks_client_secret is defined"
            ),
            input={
                "server_hostname": "{{ raw.server_hostname }}",
                "client_id": "{{ raw.databricks_client_id }}",
                "client_secret": "{{ raw.databricks_client_secret }}",
                "azure_tenant_id": "{{ raw.azure_tenant_id | default(none) }}",
                "azure_workspace_resource_id": "{{ raw.azure_workspace_resource_id | default(none) }}",
            },
            output={"credentials_provider": "databricks_credentials_provider"},
            field_map={
                "credentials_provider": "{{ derived.databricks_credentials_provider }}"
            },
        ),
    ],
    mapper=MapperConfig(
        name="databricks_sql_client_args",
        schema=DatabricksSqlClientArgs,
        field_map={
            "server_hostname": "{{ raw.server_hostname }}",
            "http_path": "{{ raw.http_path }}",
            # PAT auth — absent when using OAuth (step contributes credentials_provider instead)
            "access_token": "{{ raw.access_token | default(none) }}",
            "_use_arrow_native_complex_types": "{{ raw._use_arrow_native_complex_types | default(false) }}",
            "_user_agent_entry": "{{ raw._user_agent_entry | default(none) }}",
        },
    ),
)

# Not registered: proxy client reads credentials flat and calls the SDK itself.
# Phase 2 will register when DatabricksSqlWarehouseProxyClient reads from connect_args.


class DatabricksRestClientArgs(TypedDict):
    databricks_workspace_url: Required[str]
    databricks_token: NotRequired[str]  # PAT auth
    databricks_client_id: NotRequired[str]  # OAuth (Databricks or Azure-managed)
    databricks_client_secret: NotRequired[str]  # OAuth
    azure_tenant_id: NotRequired[str]  # Azure-managed OAuth only
    azure_workspace_resource_id: NotRequired[str]  # Azure-managed OAuth only


DATABRICKS_REST_DEFAULT_CCP = CcpConfig(
    name="databricks-rest-default",
    steps=[
        # Phase 2: add an OAuth→token transform that resolves the access token string
        # directly, so the proxy client can read connect_args["token"] without calling
        # the SDK itself.
    ],
    mapper=MapperConfig(
        name="databricks_rest_client_args",
        schema=DatabricksRestClientArgs,
        field_map={
            "databricks_workspace_url": "{{ raw.databricks_workspace_url }}",
            # PAT auth
            "databricks_token": "{{ raw.databricks_token | default(none) }}",
            # OAuth auth — proxy client resolves the token internally in Phase 1
            "databricks_client_id": "{{ raw.databricks_client_id | default(none) }}",
            "databricks_client_secret": "{{ raw.databricks_client_secret | default(none) }}",
            "azure_tenant_id": "{{ raw.azure_tenant_id | default(none) }}",
            "azure_workspace_resource_id": "{{ raw.azure_workspace_resource_id | default(none) }}",
        },
    ),
)

# Not registered: proxy client reads credentials flat and resolves the token internally.
# Phase 2 will register when DatabricksRestProxyClient reads from connect_args.
