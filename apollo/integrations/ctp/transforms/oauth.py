import base64

import requests

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry

_GRANT_TYPE_PASSWORD = "password"
_GRANT_TYPE_CLIENT_CREDENTIALS = "client_credentials"
_SUPPORTED_GRANT_TYPES = (_GRANT_TYPE_CLIENT_CREDENTIALS, _GRANT_TYPE_PASSWORD)
_REQUIRED_OAUTH_KEYS = (
    "client_id",
    "client_secret",
    "access_token_endpoint",
    "grant_type",
)


class OAuthTransform(Transform):
    """Acquire an OAuth 2.0 access token and store it in pipeline state.

    Supports client_credentials and password grant types, mirroring the behavior
    of DC's OAuthClient. Sends both an HTTP Basic Auth header and the client
    credentials as form parameters for maximum compatibility with non-standard
    authorization servers (some require the Basic header, others require form
    params; sending both satisfies either).

    Step input:
        oauth (required): template resolving to an oauth config dict containing:
            client_id (required)
            client_secret (required)
            access_token_endpoint (required)
            grant_type (required): "client_credentials" or "password"
            scope (optional): space-separated scope string
            username (required for password grant)
            password (required for password grant)

    Step output:
        token (required): derived key where the access_token string is stored.
    """

    required_input_keys = ("oauth",)
    optional_input_keys = ()
    required_output_keys = ("token",)
    optional_output_keys = ()

    def _execute(self, step: TransformStep, state: PipelineState) -> None:
        oauth_config = TemplateEngine.render(step.input["oauth"], state)

        for key in _REQUIRED_OAUTH_KEYS:
            if not oauth_config.get(key):
                raise CtpPipelineError(
                    stage="transform_input",
                    step_name=step.type,
                    message=f"'{key}' is required in oauth config",
                )

        grant_type = oauth_config["grant_type"]
        if grant_type not in _SUPPORTED_GRANT_TYPES:
            raise CtpPipelineError(
                stage="transform_input",
                step_name=step.type,
                message=(
                    f"Unsupported grant_type '{grant_type}'; "
                    f"supported: {_SUPPORTED_GRANT_TYPES}"
                ),
            )

        if grant_type == _GRANT_TYPE_PASSWORD:
            for key in ("username", "password"):
                if not oauth_config.get(key):
                    raise CtpPipelineError(
                        stage="transform_input",
                        step_name=step.type,
                        message=f"'{key}' is required in oauth config for password grant",
                    )

        client_id = oauth_config["client_id"]
        client_secret = oauth_config["client_secret"]
        token_endpoint = oauth_config["access_token_endpoint"]

        # Build form body as a dict — requests will URL-encode values automatically
        data: dict = {"grant_type": grant_type}
        if oauth_config.get("scope"):
            data["scope"] = oauth_config["scope"]
        if grant_type == _GRANT_TYPE_PASSWORD:
            data["username"] = oauth_config["username"]
            data["password"] = oauth_config["password"]

        encoded = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        headers = {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        try:
            response = requests.post(
                token_endpoint, data=data, headers=headers, timeout=30
            )
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message=f"Token endpoint returned HTTP {exc.response.status_code}",
            ) from exc
        except requests.RequestException as exc:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message=f"Token request failed: {exc}",
            ) from exc

        try:
            body = response.json()
        except ValueError as exc:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message="Token endpoint returned a non-JSON response",
            ) from exc

        if "access_token" not in body:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message=(
                    f"Token endpoint response missing 'access_token'; "
                    f"got keys: {sorted(body.keys())}"
                ),
            )

        state.derived[step.output["token"]] = body["access_token"]


TransformRegistry.register("oauth", OAuthTransform)
