from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry


class LoadPrivateKeyTransform(Transform):
    """Load a PEM private key and write DER-encoded bytes to pipeline state.

    Used by Snowflake (and any other connector that requires an unencrypted DER
    private key) when the user stores credentials as a PEM string.

    Step input:
        pem (required): template resolving to a PEM private key string or bytes.
        password (optional): template resolving to the passphrase (str or bytes).
            Omit or resolve to None for unencrypted keys.

    Step output:
        private_key (required): derived key where DER bytes are stored.

    Step field_map (typical usage):
        {"private_key": "{{ derived.<output_key> }}"}
    """

    def execute(self, step: TransformStep, state: PipelineState) -> None:
        if "pem" not in step.input:
            raise CtpPipelineError(
                stage="transform_input",
                step_name=step.type,
                message="'pem' is required in load_private_key input",
            )
        if "private_key" not in step.output:
            raise CtpPipelineError(
                stage="transform_output",
                step_name=step.type,
                message="'private_key' is required in load_private_key output",
            )

        pem = TemplateEngine.render(step.input["pem"], state)
        pem_bytes = pem.encode() if isinstance(pem, str) else pem

        password: bytes | None = None
        if "password" in step.input:
            raw_password = TemplateEngine.render(step.input["password"], state)
            if raw_password is not None:
                password = (
                    raw_password.encode()
                    if isinstance(raw_password, str)
                    else raw_password
                )

        try:
            private_key = load_pem_private_key(pem_bytes, password=password)
        except (ValueError, TypeError) as exc:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message=f"Failed to load private key: {exc}",
            ) from exc

        der_bytes = private_key.private_bytes(
            encoding=Encoding.DER,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        )
        state.derived[step.output["private_key"]] = der_bytes


TransformRegistry.register("load_private_key", LoadPrivateKeyTransform)
