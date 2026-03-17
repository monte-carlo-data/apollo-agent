# apollo/integrations/ccp/mapper.py
from typing import Any

from apollo.integrations.ccp.models import MapperConfig, PipelineState
from apollo.integrations.ccp.template import TemplateEngine


class Mapper:
    def execute(
        self,
        config: MapperConfig,
        state: PipelineState,
        step_field_maps: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if config.passthrough:
            return dict(state.raw)

        # Base field_map first, then step contributions (step takes precedence on collision)
        combined = {**config.field_map, **(step_field_maps or {})}
        result = {}
        for key, template in combined.items():
            value = TemplateEngine.render(template, state) if isinstance(template, str) else template
            if value is not None:
                result[key] = value
        return result
