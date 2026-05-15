import base64

from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry


class EncodeBasicAuthTransform(Transform):
    """Base64-encode username:password into a Basic auth token.

    Step input:
        username (required): template resolving to the username/key string.
        password (required): template resolving to the password/secret string.

    Step output:
        token (required): derived key where the base64-encoded string is stored.
    """

    required_input_keys = ("username", "password")
    optional_input_keys = ()
    required_output_keys = ("token",)
    optional_output_keys = ()

    def _execute(self, step: TransformStep, state: PipelineState) -> None:
        username = self._require(step, state, "username", "required for Basic auth")
        password = self._require(step, state, "password", "required for Basic auth")
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        state.derived[step.output["token"]] = token


TransformRegistry.register("encode_basic_auth", EncodeBasicAuthTransform)
