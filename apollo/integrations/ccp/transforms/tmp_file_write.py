import os
import tempfile

from apollo.integrations.ccp.errors import CcpPipelineError
from apollo.integrations.ccp.models import PipelineState, TransformStep
from apollo.integrations.ccp.template import TemplateEngine
from apollo.integrations.ccp.transforms.base import Transform
from apollo.integrations.ccp.transforms.registry import TransformRegistry


class TmpFileWriteTransform(Transform):
    def execute(self, step: TransformStep, state: PipelineState) -> None:
        if "contents" not in step.input:
            raise CcpPipelineError(
                stage="transform_input",
                step_name=step.type,
                message="'contents' is required in tmp_file_write input",
            )

        contents = TemplateEngine.render(step.input["contents"], state)
        file_suffix = step.input.get("file_suffix", "")
        mode_str = step.input.get("mode", "0600")
        if "path" not in step.output:
            raise CcpPipelineError(
                stage="transform_output",
                step_name=step.type,
                message="'path' is required in tmp_file_write output",
            )
        output_key = step.output["path"]

        is_bytes = isinstance(contents, bytes)
        with tempfile.NamedTemporaryFile(
            mode="wb" if is_bytes else "w", suffix=file_suffix, delete=False
        ) as f:
            f.write(contents if is_bytes else str(contents))
            path = f.name

        try:
            os.chmod(path, int(mode_str, 8))
        except Exception:
            os.unlink(path)
            raise

        # TODO: temp files are not cleaned up after the connection closes;
        # a cleanup protocol tied to the proxy client lifecycle is needed
        state.derived[output_key] = path


TransformRegistry.register("tmp_file_write", TmpFileWriteTransform)
