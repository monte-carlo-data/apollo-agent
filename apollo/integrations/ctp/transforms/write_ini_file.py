import os
import tempfile

from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry

_SECTION_KEY = "section"


class WriteIniFileTransform(Transform):
    """
    Writes key-value pairs to a temporary file in INI format and stores the path
    in ``state.derived``.

    All input keys except ``section`` are rendered as field entries under the
    named section.  ``None``-valued fields are omitted.

    Input keys:
      - ``section``: the INI section name (e.g. ``"Looker"``)
      - any additional keys: rendered as ``key=value`` lines in the section

    Output keys:
      - ``path``: key name in ``state.derived`` where the file path is stored

    Example::

        TransformStep(
            type="write_ini_file",
            input={
                "section":       "Looker",
                "base_url":      "{{ raw.base_url }}",
                "client_id":     "{{ raw.client_id }}",
                "client_secret": "{{ raw.client_secret }}",
                "verify_ssl":    "{{ raw.verify_ssl | default(true) }}",
            },
            output={"path": "looker_ini_path"},
            field_map={"ini_file_path": "{{ derived.looker_ini_path }}"},
        )
    """

    required_input_keys = (_SECTION_KEY,)
    optional_input_keys = None  # accepts arbitrary extra keys as INI field entries
    required_output_keys = ("path",)
    optional_output_keys = ()

    def _execute(self, step: TransformStep, state: PipelineState) -> None:
        output_key = step.output["path"]
        section = TemplateEngine.render(step.input[_SECTION_KEY], state)

        fields = {}
        for key, template in step.input.items():
            if key == _SECTION_KEY:
                continue
            value = TemplateEngine.render(template, state)
            if value is not None:
                fields[key] = str(value)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".ini", delete=False) as f:
            f.write(f"[{section}]\n")
            for key, value in fields.items():
                f.write(f"{key}={value}\n")

        os.chmod(f.name, 0o600)
        state.derived[output_key] = f.name


TransformRegistry.register("write_ini_file", WriteIniFileTransform)
