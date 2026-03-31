from abc import ABC, abstractmethod

from apollo.integrations.ctp.models import PipelineState, TransformStep


class Transform(ABC):
    @abstractmethod
    def execute(self, step: TransformStep, state: PipelineState) -> None:
        """
        Execute this transform. Writes output additively into state.derived.
        Must not write to state.raw or state.context.
        Raises CtpPipelineError on validation failure.
        """
