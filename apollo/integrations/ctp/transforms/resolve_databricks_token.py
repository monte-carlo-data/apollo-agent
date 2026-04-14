from typing import Optional

from databricks.sdk.core import Config, azure_service_principal, oauth_service_principal

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry


class ResolveDatabricksTokenTransform(Transform):
    """
    Resolves a Databricks access token string for use with REST API calls.

    Supports three authentication modes (checked in priority order):
      1. Databricks-managed OAuth — when ``client_id`` and ``client_secret`` are present
         (without Azure fields). Uses ``oauth_service_principal``.
      2. Azure-managed OAuth — when ``client_id``, ``client_secret``, ``azure_tenant_id``,
         and ``azure_workspace_resource_id`` are all present. Uses ``azure_service_principal``.
      3. PAT — when ``databricks_token`` is present. Stored directly with no SDK call.

    OAuth priority over PAT mirrors ``DatabricksRestProxyClient._authentication_mode`` so
    that customers who migrated from PAT to OAuth but still have stale PAT credentials don't
    accidentally authenticate with the old token.

    Input keys:
      - ``workspace_url``: Databricks workspace URL (required)
      - ``databricks_token``: personal access token (required for PAT mode)
      - ``client_id``: service principal client ID (required for OAuth modes)
      - ``client_secret``: service principal client secret (required for OAuth modes)
      - ``azure_tenant_id``: Azure tenant ID (required for Azure OAuth mode)
      - ``azure_workspace_resource_id``: Azure workspace resource ID (required for Azure OAuth)

    Output keys:
      - ``token``: key in ``state.derived`` where the resolved token string is stored
    """

    def execute(self, step: TransformStep, state: PipelineState) -> None:
        output_key = step.output.get("token")
        if not output_key:
            raise CtpPipelineError(
                stage="transform_output",
                step_name=step.type,
                message="'token' output key required",
            )

        workspace_url = TemplateEngine.render(
            step.input.get("workspace_url", "{{ none }}"), state
        )
        if not workspace_url:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message="'workspace_url' must not be empty",
            )

        client_id = TemplateEngine.render(
            step.input.get("client_id", "{{ none }}"), state
        )
        client_secret = TemplateEngine.render(
            step.input.get("client_secret", "{{ none }}"), state
        )
        azure_tenant_id = TemplateEngine.render(
            step.input.get("azure_tenant_id", "{{ none }}"), state
        )
        azure_workspace_resource_id = TemplateEngine.render(
            step.input.get("azure_workspace_resource_id", "{{ none }}"), state
        )
        databricks_token = TemplateEngine.render(
            step.input.get("databricks_token", "{{ none }}"), state
        )

        token = self._resolve_token(
            workspace_url=workspace_url,
            client_id=client_id,
            client_secret=client_secret,
            azure_tenant_id=azure_tenant_id,
            azure_workspace_resource_id=azure_workspace_resource_id,
            databricks_token=databricks_token,
            step_name=step.type,
        )
        state.derived[output_key] = token

    @staticmethod
    def _resolve_token(
        workspace_url: str,
        client_id: Optional[str],
        client_secret: Optional[str],
        azure_tenant_id: Optional[str],
        azure_workspace_resource_id: Optional[str],
        databricks_token: Optional[str],
        step_name: str,
    ) -> str:
        # OAuth takes priority over PAT — customers who migrated may have stale PAT present.
        if client_id and client_secret:
            is_azure = bool(azure_tenant_id and azure_workspace_resource_id)
            if is_azure:
                config = Config(
                    host=workspace_url,
                    azure_client_id=client_id,
                    azure_client_secret=client_secret,
                    azure_tenant_id=azure_tenant_id,
                    azure_workspace_resource_id=azure_workspace_resource_id,
                )
                provider = azure_service_principal
            else:
                config = Config(
                    host=workspace_url,
                    client_id=client_id,
                    client_secret=client_secret,
                )
                provider = oauth_service_principal

            header_factory = provider(config)
            auth_header = header_factory().get("Authorization", "")
            token = auth_header.removeprefix("Bearer ").strip()
            if not token:
                raise CtpPipelineError(
                    stage="transform_execute",
                    step_name=step_name,
                    message="Databricks OAuth provider returned an empty token",
                )
            return token

        if databricks_token:
            return databricks_token

        raise CtpPipelineError(
            stage="transform_execute",
            step_name=step_name,
            message=(
                "No supported Databricks credentials found. "
                "Provide 'client_id'+'client_secret' for OAuth or 'databricks_token' for PAT."
            ),
        )


TransformRegistry.register("resolve_databricks_token", ResolveDatabricksTokenTransform)
