from abc import ABC, abstractmethod

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep


class Transform(ABC):
    """Base class for CTP pipeline transforms.

    Subclasses declare which ``step.input`` and ``step.output`` keys they
    require by setting ``required_input_keys`` and ``required_output_keys``.
    The base ``execute()`` validates both before delegating to ``_execute()``.
    """

    required_input_keys: tuple[str, ...] = ()
    required_output_keys: tuple[str, ...] = ()
    # Declare to enable unknown-key validation: allowed = required ∪ optional.
    # Leave as None to skip unknown-key checks (for passthrough transforms that
    # accept arbitrary extra keys, e.g. write_ini_file).
    optional_input_keys: tuple[str, ...] | None = None
    optional_output_keys: tuple[str, ...] | None = None

    def execute(self, step: TransformStep, state: PipelineState) -> None:
        """Validate required and (if declared) unknown keys, then delegate to _execute."""
        for key in self.required_input_keys:
            if key not in step.input:
                raise CtpPipelineError(
                    stage="transform_input",
                    step_name=step.type,
                    message=f"'{key}' is required in {step.type} input",
                )
        if self.optional_input_keys is not None:
            allowed = set(self.required_input_keys) | set(self.optional_input_keys)
            unknown = set(step.input) - allowed
            if unknown:
                raise CtpPipelineError(
                    stage="transform_input",
                    step_name=step.type,
                    message=f"Unknown input keys for {step.type}: {sorted(unknown)}",
                )
        for key in self.required_output_keys:
            if key not in step.output:
                raise CtpPipelineError(
                    stage="transform_output",
                    step_name=step.type,
                    message=f"'{key}' is required in {step.type} output",
                )
        if self.optional_output_keys is not None:
            allowed = set(self.required_output_keys) | set(self.optional_output_keys)
            unknown = set(step.output) - allowed
            if unknown:
                raise CtpPipelineError(
                    stage="transform_output",
                    step_name=step.type,
                    message=f"Unknown output keys for {step.type}: {sorted(unknown)}",
                )
        self._execute(step, state)

    @abstractmethod
    def _execute(self, step: TransformStep, state: PipelineState) -> None:
        """Execute this transform. Writes output additively into state.derived.
        Must not write to state.raw or state.context.
        Raises CtpPipelineError on failure.
        """
