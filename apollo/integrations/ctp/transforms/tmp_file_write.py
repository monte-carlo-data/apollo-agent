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

        # Record the path so the proxy client can delete it on close — the
        # pipeline has no handle to the (not-yet-constructed) client itself.
        state.temp_files.append(path)
        state.derived[output_key] = path


TransformRegistry.register("tmp_file_write", TmpFileWriteTransform)
