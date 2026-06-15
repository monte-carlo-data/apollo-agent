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
        temp_files: list[str] | None = None,
    ) -> dict[str, Any]:
        # Copy raw_credentials so state.raw is an independent dict — the
        # clear() at the end of this method must not mutate the caller's dict.
        #
        # temp_files is an out-param: when the caller passes a list, transforms
        # append the filesystem paths they create to it so the caller (the
        # proxy client factory) can hand them to the client for deletion on
        # close. Defaults to a throwaway list when omitted (e.g. in unit tests).
        state = PipelineState(
            raw=dict(raw_credentials),
            context=context or {},
            temp_files=temp_files if temp_files is not None else [],
        )
        step_field_maps: dict[str, Any] = {}

        try:
            for step in config.steps:
                if step.when is not None:
                    if not TemplateEngine.evaluate_condition(step.when, state):
                        continue
                transform = TransformRegistry.get(step.type)
                transform.execute(step, state)
                step_field_maps.update(step.field_map)

            result = self._mapper.execute(
                config.mapper, state, step_field_maps=step_field_maps
            )
            # Merge connector-level defaults under the mapper output so that
            # static constants (e.g. http_scheme, keepalives) survive CTP
            # replacement even when a custom mapper omits them.  Mapper wins.
            if config.connect_args_defaults:
                result = {**config.connect_args_defaults, **result}
        finally:
            # Scrub credential state regardless of success or failure so that raw
            # credentials and derived secrets (tokens, keys) cannot leak into
            # error payloads, Sentry context, or any reference that outlives
            # this call.
            state.raw.clear()
            state.derived.clear()
        return result
