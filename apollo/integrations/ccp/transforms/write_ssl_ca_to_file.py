import hashlib

from apollo.integrations.ccp.errors import CcpPipelineError
from apollo.integrations.ccp.models import PipelineState, TransformStep
from apollo.integrations.ccp.template import TemplateEngine
from apollo.integrations.ccp.transforms.base import Transform
from apollo.integrations.ccp.transforms.registry import TransformRegistry
from apollo.integrations.db.ssl_options import SslOptions


class WriteSslCaToFileTransform(Transform):
    """Write a CA certificate string to a deterministic temp file.

    Uses SslOptions.write_ca_data_to_temp_file with upsert=True, matching
    the semantics used by proxy clients: the same CA content always resolves
    to the same path, so repeated connections don't create new files.

    Step input:
        ca_data (required): template expression resolving to a PEM certificate string.

    Step output:
        path (required): derived key name to store the cert file path under.
    """

    def execute(self, step: TransformStep, state: PipelineState) -> None:
        if "ca_data" not in step.input:
            raise CcpPipelineError(
                stage="transform_input",
                step_name=step.type,
                message="'ca_data' is required in write_ssl_ca_to_file input",
            )
        if "path" not in step.output:
            raise CcpPipelineError(
                stage="transform_output",
                step_name=step.type,
                message="'path' is required in write_ssl_ca_to_file output",
            )

        ca_data = TemplateEngine.render(step.input["ca_data"], state)
        ssl_options = SslOptions(ca_data=ca_data)

        content_hash = hashlib.sha256(ca_data.encode()).hexdigest()[:12]
        cert_path = f"/tmp/{content_hash}_ssl_ca.pem"

        ssl_options.write_ca_data_to_temp_file(cert_path, upsert=True)

        state.derived[step.output["path"]] = cert_path


TransformRegistry.register("write_ssl_ca_to_file", WriteSslCaToFileTransform)
