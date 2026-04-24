import os
import tempfile

from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry


class TmpFileWriteTransform(Transform):
    required_input_keys = ("contents",)
    optional_input_keys = ("file_suffix", "mode")
    required_output_keys = ("path",)
    optional_output_keys = ()

    def _execute(self, step: TransformStep, state: PipelineState) -> None:
        contents = TemplateEngine.render(step.input["contents"], state)
        file_suffix = step.input.get("file_suffix", "")
        mode_str = step.input.get("mode", "0600")
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
