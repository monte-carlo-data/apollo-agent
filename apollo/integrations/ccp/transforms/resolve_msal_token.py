from typing import Optional

import msal

from apollo.integrations.ccp.errors import CcpPipelineError
from apollo.integrations.ccp.models import PipelineState, TransformStep
from apollo.integrations.ccp.template import TemplateEngine
from apollo.integrations.ccp.transforms.base import Transform
from apollo.integrations.ccp.transforms.registry import TransformRegistry

_AUTHORITY_URL_PREFIX = "https://login.microsoftonline.com/"
_POWERBI_SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]

_AUTH_MODE_SERVICE_PRINCIPAL = "service_principal"
_AUTH_MODE_PRIMARY_USER = "primary_user"
_SUPPORTED_AUTH_MODES = (_AUTH_MODE_SERVICE_PRINCIPAL, _AUTH_MODE_PRIMARY_USER)


class ResolveMsalTokenTransform(Transform):
    """
    Acquires a Microsoft identity platform (MSAL) access token.

    Mirrors the ``_get_access_token`` / ``_auth_as_service_principal`` /
    ``_auth_as_primary_user`` functions in ``powerbi_proxy_client.py`` so that
    Phase 2 can replace the proxy-client call with this transform.

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

    def execute(self, step: TransformStep, state: PipelineState) -> None:
        output_key = step.output.get("token")
        if not output_key:
            raise CcpPipelineError(
                stage="transform_output",
                step_name=step.type,
                message="'token' output key required",
            )

        auth_mode = TemplateEngine.render(
            step.input.get("auth_mode", "{{ none }}"), state
        )
        if auth_mode not in _SUPPORTED_AUTH_MODES:
            raise CcpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message=(
                    f"Unsupported auth_mode: '{auth_mode}'. "
                    f"Expected one of: {_SUPPORTED_AUTH_MODES}"
                ),
            )

        client_id = TemplateEngine.render(
            step.input.get("client_id", "{{ none }}"), state
        )
        tenant_id = TemplateEngine.render(
            step.input.get("tenant_id", "{{ none }}"), state
        )

        for key, value in (("client_id", client_id), ("tenant_id", tenant_id)):
            if not value:
                raise CcpPipelineError(
                    stage="transform_execute",
                    step_name=step.type,
                    message=f"'{key}' is required in resolve_msal_token input",
                )

        authority = f"{_AUTHORITY_URL_PREFIX}{tenant_id}"

        if auth_mode == _AUTH_MODE_SERVICE_PRINCIPAL:
            client_secret = TemplateEngine.render(
                step.input.get("client_secret", "{{ none }}"), state
            )
            if not client_secret:
                raise CcpPipelineError(
                    stage="transform_execute",
                    step_name=step.type,
                    message="'client_secret' is required in resolve_msal_token input for service_principal auth_mode",
                )
            token = self._service_principal_token(
                client_id, client_secret, authority, _POWERBI_SCOPES
            )
        else:
            username = TemplateEngine.render(
                step.input.get("username", "{{ none }}"), state
            )
            password = TemplateEngine.render(
                step.input.get("password", "{{ none }}"), state
            )
            for key, value in (("username", username), ("password", password)):
                if not value:
                    raise CcpPipelineError(
                        stage="transform_execute",
                        step_name=step.type,
                        message=f"'{key}' is required in resolve_msal_token input for primary_user auth_mode",
                    )
            token = self._primary_user_token(
                client_id, authority, username, password, _POWERBI_SCOPES
            )

        state.derived[output_key] = token

    @staticmethod
    def _service_principal_token(
        client_id: str, client_secret: str, authority: str, scopes: list
    ) -> str:
        app = msal.ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=authority,
        )
        response = app.acquire_token_for_client(scopes=scopes)
        ResolveMsalTokenTransform._raise_for_response(response)
        assert response is not None
        return response["access_token"]

    @staticmethod
    def _primary_user_token(
        client_id: str,
        authority: str,
        username: str,
        password: str,
        scopes: list,
    ) -> str:
        app = msal.PublicClientApplication(client_id, authority=authority)
        accounts = app.get_accounts(username=username)

        response: Optional[dict] = None
        if accounts:
            response = app.acquire_token_silent(scopes=scopes, account=accounts[0])
        if not response:
            response = app.acquire_token_by_username_password(
                username=username, password=password, scopes=scopes
            )

        ResolveMsalTokenTransform._raise_for_response(response)
        assert response is not None
        return response["access_token"]

    @staticmethod
    def _raise_for_response(response: Optional[dict]) -> None:
        if not response:
            raise CcpPipelineError(
                stage="transform_execute",
                step_name="resolve_msal_token",
                message="MSAL acquire token response is empty",
            )
        error = response.get("error")
        if error:
            raise CcpPipelineError(
                stage="transform_execute",
                step_name="resolve_msal_token",
                message=f"MSAL error: {error} ({response.get('error_description')})",
            )


TransformRegistry.register("resolve_msal_token", ResolveMsalTokenTransform)
