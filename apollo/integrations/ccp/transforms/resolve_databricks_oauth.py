from typing import Callable

from databricks.sdk.core import Config, azure_service_principal, oauth_service_principal

from apollo.integrations.ccp.errors import CcpPipelineError
from apollo.integrations.ccp.models import PipelineState, TransformStep
from apollo.integrations.ccp.template import TemplateEngine
from apollo.integrations.ccp.transforms.base import Transform
from apollo.integrations.ccp.transforms.registry import TransformRegistry

_REQUIRED_INPUTS = ("server_hostname", "client_id", "client_secret")


class ResolveDatabricksOauthTransform(Transform):
    """
    Builds a Databricks OAuth credentials provider callable for ``databricks-sql-connector``.

    Mirrors ``DatabricksSqlWarehouseProxyClient._oauth_credentials_provider()`` so that
    Phase 2 can replace the proxy-client call with this transform.

    Azure-managed OAuth is selected when both ``azure_tenant_id`` and
    ``azure_workspace_resource_id`` are present and non-empty; otherwise
    Databricks-managed OAuth is used.

    Input keys:
      - ``server_hostname``: Databricks workspace hostname (no scheme)
      - ``client_id``: service principal client ID (``databricks_client_id``)
      - ``client_secret``: service principal secret (``databricks_client_secret``)
      - ``azure_tenant_id``: Azure tenant ID (optional; triggers Azure-managed OAuth)
      - ``azure_workspace_resource_id``: Azure workspace resource ID (optional)

    Output keys:
      - ``credentials_provider``: key in ``state.derived`` where the callable is stored

    The stored value is ``lambda: provider(config)`` — a zero-argument callable that
    ``databricks-sql-connector`` invokes when it needs fresh credentials.
    """

    def execute(self, step: TransformStep, state: PipelineState) -> None:
        for key in _REQUIRED_INPUTS:
            if key not in step.input:
                raise CcpPipelineError(
                    stage="transform_input",
                    step_name=step.type,
                    message=f"'{key}' key required in step input",
                )

        output_key = step.output.get("credentials_provider")
        if not output_key:
            raise CcpPipelineError(
                stage="transform_output",
                step_name=step.type,
                message="'credentials_provider' output key required",
            )

        host = TemplateEngine.render(step.input["server_hostname"], state)
        client_id = TemplateEngine.render(step.input["client_id"], state)
        client_secret = TemplateEngine.render(step.input["client_secret"], state)

        for key, value in (
            ("server_hostname", host),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ):
            if not value:
                raise CcpPipelineError(
                    stage="transform_execute",
                    step_name=step.type,
                    message=f"'{key}' must not be empty",
                )

        azure_tenant_id = None
        azure_workspace_resource_id = None
        if "azure_tenant_id" in step.input:
            azure_tenant_id = TemplateEngine.render(
                step.input["azure_tenant_id"], state
            )
        if "azure_workspace_resource_id" in step.input:
            azure_workspace_resource_id = TemplateEngine.render(
                step.input["azure_workspace_resource_id"], state
            )

        is_azure = bool(azure_tenant_id and azure_workspace_resource_id)

        if is_azure:
            config = Config(
                host=host,
                azure_client_id=client_id,
                azure_client_secret=client_secret,
                azure_tenant_id=azure_tenant_id,
                azure_workspace_resource_id=azure_workspace_resource_id,
            )
            provider = azure_service_principal
        else:
            config = Config(
                host=host,
                client_id=client_id,
                client_secret=client_secret,
            )
            provider = oauth_service_principal

        state.derived[output_key] = _make_provider(provider, config)


def _make_provider(provider: Callable, config: Config) -> Callable:
    """Return a zero-argument callable that invokes ``provider(config)``."""
    return lambda: provider(config)


TransformRegistry.register("resolve_databricks_oauth", ResolveDatabricksOauthTransform)
