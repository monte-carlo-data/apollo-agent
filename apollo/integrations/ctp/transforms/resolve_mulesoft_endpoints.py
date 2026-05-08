from urllib.parse import urlsplit

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry

_REGION_TO_AUTH_URL = {
    "US": "https://anypoint.mulesoft.com/accounts/api/v2/oauth2/token",
    "EU": "https://eu1.anypoint.mulesoft.com/accounts/api/v2/oauth2/token",
    "Gov": "https://mpt.mulesoft.com/accounts/api/v2/oauth2/token",
}
_ALLOWED_HOSTS = frozenset(
    {
        "anypoint.mulesoft.com",
        "eu1.anypoint.mulesoft.com",
        "mpt.mulesoft.com",
    }
)
_DEFAULT_REGION = "US"


class ResolveMulesoftEndpointsTransform(Transform):
    """Resolve MuleSoft Anypoint endpoints from a region (or overrides) and build
    the OAuth config dict consumed by the shared ``oauth`` transform.

    Performs no outbound HTTP. Maps the requested region to MuleSoft's OAuth token
    endpoint, and applies HTTPS-plus-allowlist validation to an optional ``auth_url``
    override.

    Step input keys (all optional — values rendered through the template engine):
        client_id      : Anypoint connected app client ID (required value)
        client_secret  : Connected app client secret (required value)
        region         : "US" | "EU" | "Gov" (default "US")
        auth_url       : Override for the OAuth token endpoint

    Step output keys:
        oauth_config   : derived key for the dict consumed by the shared ``oauth``
                         transform (grant_type, client_id, client_secret,
                         access_token_endpoint)
    """

    optional_input_keys = (
        "client_id",
        "client_secret",
        "region",
        "auth_url",
    )
    required_output_keys = ("oauth_config",)
    optional_output_keys = ()

    def _execute(self, step: TransformStep, state: PipelineState) -> None:
        client_id = self._require(
            step, state, "client_id", "required for MuleSoft OAuth"
        )
        client_secret = self._require(
            step, state, "client_secret", "required for MuleSoft OAuth"
        )

        region = (
            TemplateEngine.render(step.input.get("region", "{{ none }}"), state)
            or _DEFAULT_REGION
        )
        if region not in _REGION_TO_AUTH_URL:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message=(
                    f"Unsupported MuleSoft region: '{region}'. "
                    f"Expected one of {sorted(_REGION_TO_AUTH_URL.keys())}."
                ),
            )

        auth_url_override = TemplateEngine.render(
            step.input.get("auth_url", "{{ none }}"), state
        )

        auth_url = (
            self._validate_override_url(auth_url_override, step.type)
            if auth_url_override
            else _REGION_TO_AUTH_URL[region]
        )

        oauth_config = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "access_token_endpoint": auth_url,
        }

        state.derived[step.output["oauth_config"]] = oauth_config

    @staticmethod
    def _validate_override_url(url: str, step_name: str) -> str:
        # Without this check, an attacker who can inject auth_url could redirect
        # the OAuth POST (which sends client_id/client_secret in the body) to a
        # host they control.
        parts = urlsplit(url)
        if parts.scheme != "https":
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step_name,
                message=(f"Override URL must use https scheme; got '{parts.scheme}'."),
            )
        host = parts.hostname or ""
        if host not in _ALLOWED_HOSTS:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step_name,
                message=(
                    f"Override URL host '{host}' is not in the MuleSoft Anypoint "
                    f"allowlist {sorted(_ALLOWED_HOSTS)}."
                ),
            )
        return url


TransformRegistry.register(
    "resolve_mulesoft_endpoints", ResolveMulesoftEndpointsTransform
)
