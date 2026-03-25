# apollo/integrations/ctp/pipeline.py
from typing import Any

from apollo.integrations.ctp.mapper import Mapper
from apollo.integrations.ctp.models import CtpConfig, PipelineState
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms.registry import TransformRegistry


class CtpPipeline:
    def __init__(self):
        self._mapper = Mapper()

    def execute(
        self,
        config: CtpConfig,
        raw_credentials: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        state = PipelineState(raw=raw_credentials, context=context or {})
        step_field_maps: dict[str, Any] = {}

        for step in config.steps:
            if step.when is not None:
                if not TemplateEngine.evaluate_condition(step.when, state):
                    continue
            transform = TransformRegistry.get(step.type)
            transform.execute(step, state)
            step_field_maps.update(step.field_map)

        return self._mapper.execute(
            config.mapper, state, step_field_maps=step_field_maps
        )
