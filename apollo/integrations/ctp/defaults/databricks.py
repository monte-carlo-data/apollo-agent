from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep
from apollo.integrations.ctp.registry import CtpRegistry


class DatabricksSqlClientArgs(TypedDict):
    server_hostname: Required[str]
    http_path: Required[str]
    access_token: NotRequired[str]  # PAT auth
    credentials_provider: NotRequired[
        Any
    ]  # OAuth callable from resolve_databricks_oauth
    _use_arrow_native_complex_types: NotRequired[bool]
    _user_agent_entry: NotRequired[str]


DATABRICKS_DEFAULT_CTP = CtpConfig(
    name="databricks-default",
    steps=[
        # OAuth path: build credentials_provider callable and contribute it to connect_args.
        # databricks_client_id / databricks_client_secret are intentionally excluded from the
        # mapper field_map so they are not passed to sql.connect, which means the proxy
        # client's own _credentials_use_oauth check is False and it will not overwrite the
        # CTP-provided callable.
        TransformStep(
            type="resolve_databricks_oauth",
            when=(
                "raw.databricks_client_id is defined"
                " and raw.databricks_client_secret is defined"
            ),
            input={
                "server_hostname": "{{ raw.server_hostname if raw.server_hostname is defined else (raw.databricks_workspace_url | replace('https://', '') | replace('http://', '') | trim('/')) }}",
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
            "server_hostname": "{{ raw.server_hostname if raw.server_hostname is defined else (raw.databricks_workspace_url | replace('https://', '') | replace('http://', '') | trim('/')) }}",
            "http_path": "{{ raw.http_path }}",
            # PAT auth — absent when using OAuth (step contributes credentials_provider instead)
            "access_token": "{{ raw.access_token | default(none) }}",
            "_use_arrow_native_complex_types": "{{ raw._use_arrow_native_complex_types | default(none) }}",
            "_user_agent_entry": "{{ raw._user_agent_entry | default(none) }}",
        },
    ),
    # Disable Arrow native complex types — required for correct behaviour with the
    # Databricks SQL connector; injected as a default so custom CTP configs inherit it.
    connect_args_defaults={"_use_arrow_native_complex_types": False},
)

CtpRegistry.register("databricks", DATABRICKS_DEFAULT_CTP)


class DatabricksRestClientArgs(TypedDict):
    databricks_workspace_url: Required[str]
    token: Required[str]  # resolved by resolve_databricks_token (PAT or OAuth)


DATABRICKS_REST_DEFAULT_CTP = CtpConfig(
    name="databricks-rest-default",
    steps=[
        # Resolve the access token for all auth modes (PAT, Databricks OAuth, Azure OAuth).
        # OAuth keys take priority over PAT — see ResolveDatabricksTokenTransform for details.
        # Skipped on the DC pre-shaped path where the token is already resolved in raw.token.
        TransformStep(
            type="resolve_databricks_token",
            when="raw.databricks_token is defined or raw.databricks_client_id is defined",
            input={
                "workspace_url": "{{ raw.databricks_workspace_url }}",
                "databricks_token": "{{ raw.databricks_token | default(none) }}",
                "client_id": "{{ raw.databricks_client_id | default(none) }}",
                "client_secret": "{{ raw.databricks_client_secret | default(none) }}",
                "azure_tenant_id": "{{ raw.azure_tenant_id | default(none) }}",
                "azure_workspace_resource_id": "{{ raw.azure_workspace_resource_id | default(none) }}",
            },
            output={"token": "databricks_rest_token"},
            field_map={"token": "{{ derived.databricks_rest_token }}"},
        ),
    ],
    mapper=MapperConfig(
        name="databricks_rest_client_args",
        schema=DatabricksRestClientArgs,
        field_map={
            "databricks_workspace_url": "{{ raw.databricks_workspace_url }}",
            # On the flat path, token is contributed by the resolve_databricks_token step above.
            # On the DC pre-shaped path (step skipped), token is read directly from raw.
            "token": "{{ raw.token | default(none) }}",
        },
    ),
)

CtpRegistry.register("databricks-rest", DATABRICKS_REST_DEFAULT_CTP)
