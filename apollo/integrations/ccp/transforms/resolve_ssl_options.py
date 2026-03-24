import hashlib

from apollo.integrations.ccp.errors import CcpPipelineError
from apollo.integrations.ccp.models import PipelineState, TransformStep
from apollo.integrations.ccp.template import TemplateEngine
from apollo.integrations.ccp.transforms.base import Transform
from apollo.integrations.ccp.transforms.registry import TransformRegistry
from apollo.integrations.db.ssl_options import SslOptions


class ResolveSslOptionsTransform(Transform):
    """Create a SslOptions dataclass from a raw ssl_options dict and resolve
    its derived values into the pipeline state.

    Mirrors the pattern used in proxy client __init__ methods: build SslOptions,
    write ca_data to a deterministic temp file if present, create an ssl.SSLContext
    if cert_data is present. The SslOptions object is stored in derived so the
    mapper and step field_maps can access its attributes (e.g.
    ``derived.ssl_options.disabled``).

    Step input:
        ssl_options (required): template resolving to an ssl_options dict.

    Step output — all keys are optional; omit to skip storing that value:
        ssl_options:  derived key for the SslOptions object itself.
        ca_path:      derived key for the cert file path (only written when
                      ca_data is present and SSL is not disabled).
        ssl_context:  derived key for the ssl.SSLContext (only built when
                      cert_data is present).
    """

    def execute(self, step: TransformStep, state: PipelineState) -> None:
        if "ssl_options" not in step.input:
            raise CcpPipelineError(
                stage="transform_input",
                step_name=step.type,
                message="'ssl_options' is required in resolve_ssl_options input",
            )

        ssl_options_raw = TemplateEngine.render(step.input["ssl_options"], state)
        if not isinstance(ssl_options_raw, dict):
            raise CcpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message=f"'ssl_options' must resolve to a dict, got {type(ssl_options_raw).__name__}",
            )
        ssl_options = SslOptions(**ssl_options_raw)

        if "ssl_options" in step.output:
            state.derived[step.output["ssl_options"]] = ssl_options

        if (
            "ca_path" in step.output
            and ssl_options.ca_data
            and not ssl_options.disabled
        ):
            ca_bytes = (
                ssl_options.ca_data
                if isinstance(ssl_options.ca_data, bytes)
                else ssl_options.ca_data.encode()
            )
            content_hash = hashlib.sha256(ca_bytes).hexdigest()[:12]
            cert_path = f"/tmp/{content_hash}_ssl_ca.pem"
            ssl_options.write_ca_data_to_temp_file(cert_path, upsert=True)
            state.derived[step.output["ca_path"]] = cert_path

        if "ssl_context" in step.output:
            state.derived[step.output["ssl_context"]] = ssl_options.get_ssl_context()


TransformRegistry.register("resolve_ssl_options", ResolveSslOptionsTransform)
