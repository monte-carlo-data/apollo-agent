import logging
from typing import Callable

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms._oauth_cache import (
    cache_stats,
    cached_header_factory,
)
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry

logger = logging.getLogger(__name__)


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

    The stored value is ``lambda: header_factory`` — a zero-argument callable that
    ``databricks-sql-connector`` invokes when it needs fresh credentials. The
    HeaderFactory itself is cached across operations by ``_oauth_cache`` so the
    Databricks SDK's per-Config TokenSource cache survives across calls.
    """

    required_input_keys = ("server_hostname", "client_id", "client_secret")
    optional_input_keys = ("azure_tenant_id", "azure_workspace_resource_id")
    required_output_keys = ("credentials_provider",)
    optional_output_keys = ()

    def _execute(self, step: TransformStep, state: PipelineState) -> None:
        output_key = step.output["credentials_provider"]

        host = TemplateEngine.render(step.input["server_hostname"], state)
        client_id = TemplateEngine.render(step.input["client_id"], state)
        client_secret = TemplateEngine.render(step.input["client_secret"], state)

        for key, value in (
            ("server_hostname", host),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ):
            if not value:
                raise CtpPipelineError(
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

        header_factory = cached_header_factory(
            host,
            client_id,
            client_secret,
            azure_tenant_id or None,
            azure_workspace_resource_id or None,
        )
        stats = cache_stats()
        logger.info(
            f"Resolved Databricks OAuth credentials_provider, "
            f"cache_hits={stats['hits']}, "
            f"cache_misses={stats['misses']}, "
            f"cache_size={stats['size']}"
        )
        state.derived[output_key] = _wrap_factory(header_factory)


def _wrap_factory(
    header_factory: Callable[[], dict]
) -> Callable[[], Callable[[], dict]]:
    """Return a zero-argument callable that yields the (cached) HeaderFactory.

    The ``databricks-sql-connector`` invokes the stored credentials_provider
    once per ``sql.connect()`` to obtain a HeaderFactory it then uses to fetch
    auth headers. By returning the *same* cached HeaderFactory each time
    (rather than rebuilding one via ``provider(config)``), the Databricks SDK's
    internal TokenSource cache survives across operations.
    """
    return lambda: header_factory


TransformRegistry.register("resolve_databricks_oauth", ResolveDatabricksOauthTransform)
