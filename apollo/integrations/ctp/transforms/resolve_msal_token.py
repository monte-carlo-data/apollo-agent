from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry
from apollo.integrations.powerbi.msal_auth import (
    POWERBI_SCOPE,
    MsalAuthError,
    acquire_token,
)


class ResolveMsalTokenTransform(Transform):
    """
    Acquires a Microsoft identity platform (MSAL) access token for the Power BI API scope.

    Thin CTP wrapper over :func:`apollo.integrations.powerbi.msal_auth.acquire_token` — the MSAL
    acquisition logic is shared with :class:`PowerBiTokenProvider` (the proxy client's host-based
    token selector) so it lives in one place.

    Input keys:
      - ``auth_mode``: ``"service_principal"`` or ``"primary_user"``
      - ``client_id``: Azure AD application (client) ID
      - ``tenant_id``: Azure AD tenant ID
      - ``client_secret``: app secret (required for ``service_principal``)
      - ``username``: UPN / email (required for ``primary_user``)
      - ``password``: user password (required for ``primary_user``)

    Output keys:
      - ``token``: key name in ``state.derived`` where the access token string is stored
    """

    optional_input_keys = (
        "auth_mode",
        "client_id",
        "tenant_id",
        "client_secret",
        "username",
        "password",
    )
    required_output_keys = ("token",)
    optional_output_keys = ()

    def _execute(self, step: TransformStep, state: PipelineState) -> None:
        output_key = step.output["token"]

        def _render(key: str):
            return TemplateEngine.render(step.input.get(key, "{{ none }}"), state)

        try:
            token = acquire_token(
                auth_mode=_render("auth_mode"),
                client_id=_render("client_id"),
                tenant_id=_render("tenant_id"),
                scopes=[POWERBI_SCOPE],
                client_secret=_render("client_secret"),
                username=_render("username"),
                password=_render("password"),
            )
        except MsalAuthError as e:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message=str(e),
            ) from e

        state.derived[output_key] = token


TransformRegistry.register("resolve_msal_token", ResolveMsalTokenTransform)
