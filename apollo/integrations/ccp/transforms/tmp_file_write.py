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

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=file_suffix, delete=False
        ) as f:
            f.write(str(contents))
            path = f.name

        os.chmod(path, int(mode_str, 8))
        state.derived[output_key] = path


TransformRegistry.register("tmp_file_write", TmpFileWriteTransform)
