import uuid
from datetime import datetime, timedelta

import jwt

from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry

_DEFAULT_EXPIRATION_SECONDS = 60 * 5  # 5 minutes

# Scopes required by Tableau Connected Apps
_TABLEAU_SCOPES = [
    "tableau:content:read",
    "tableau:users:read",
    "tableau:labels:read",
    "tableau:labels:update",
    "tableau:labels:create",
]


class GenerateJwtTransform(Transform):
    """
    Generates a HS256 JWT for Tableau Connected Apps authentication.

    Mirrors ``tableau_proxy_client.generate_jwt()`` exactly so that Phase 2
    can replace the proxy-client call with this transform.

    Input keys:
      - ``username``: Tableau username embedded in the ``sub`` claim
      - ``client_id``: Connected App client UUID (``iss`` claim and JWT header)
      - ``secret_id``: Connected App secret UUID (``kid`` JWT header)
      - ``secret_value``: Connected App secret value used to sign the token
      - ``expiration_seconds``: token lifetime in seconds (optional, default 300)

    Output keys:
      - ``token``: key name in ``state.derived`` where the signed JWT string is stored
    """

    required_input_keys = ("username", "client_id", "secret_id", "secret_value")
    optional_input_keys = ("expiration_seconds",)
    required_output_keys = ("token",)
    optional_output_keys = ()

    def _execute(self, step: TransformStep, state: PipelineState) -> None:
        output_key = step.output["token"]

        username = TemplateEngine.render(step.input["username"], state)
        client_id = TemplateEngine.render(step.input["client_id"], state)
        secret_id = TemplateEngine.render(step.input["secret_id"], state)
        secret_value = TemplateEngine.render(step.input["secret_value"], state)

        expiration_seconds = _DEFAULT_EXPIRATION_SECONDS
        if "expiration_seconds" in step.input:
            val = TemplateEngine.render(step.input["expiration_seconds"], state)
            if val is not None:
                expiration_seconds = int(val)

        token_id = str(uuid.uuid4())
        expires_at = datetime.utcnow() + timedelta(seconds=expiration_seconds)

        headers = {"iss": client_id, "kid": secret_id}
        payload = {
            "iss": client_id,
            "exp": expires_at,
            "jti": token_id,
            "aud": "tableau",
            "sub": username,
            "scp": _TABLEAU_SCOPES,
        }

        state.derived[output_key] = jwt.encode(
            payload=payload,
            key=secret_value,
            algorithm="HS256",
            headers=headers,
        )


TransformRegistry.register("generate_jwt", GenerateJwtTransform)
