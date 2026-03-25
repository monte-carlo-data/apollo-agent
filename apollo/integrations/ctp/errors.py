# apollo/integrations/ctp/errors.py


class CtpPipelineError(Exception):
    """Raised when the CTP pipeline fails at a specific stage."""

    def __init__(self, stage: str, message: str, step_name: str = ""):
        self.stage = stage
        self.step_name = step_name
        super().__init__(
            f"[{stage}] {f'(step: {step_name}) ' if step_name else ''}{message}"
        )
