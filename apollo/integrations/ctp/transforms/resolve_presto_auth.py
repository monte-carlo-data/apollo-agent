import prestodb

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry


class ResolvePrestoAuthTransform(Transform):
    """
    Constructs a ``prestodb.auth.BasicAuthentication`` object from a raw auth dict
    containing ``username`` and ``password`` keys.

    Input keys:
      - ``auth``: template resolving to a dict with ``username`` and ``password``

    Output keys:
      - ``auth``: key name in ``state.derived`` where the object is stored

    When the ``when`` guard evaluates to False the step is skipped and no auth
    object is produced.
    """

    required_input_keys = ("auth",)
    optional_input_keys = ()
    required_output_keys = ("auth",)
    optional_output_keys = ()

    def _execute(self, step: TransformStep, state: PipelineState) -> None:
        auth_config = TemplateEngine.render(step.input["auth"], state)

        if not auth_config:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message="'auth' input resolved to empty — check the 'when' guard",
            )

        if not isinstance(auth_config, dict):
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message=f"'auth' must be a dict, got {type(auth_config).__name__}",
            )

        for required_key in ("username", "password"):
            if required_key not in auth_config:
                raise CtpPipelineError(
                    stage="transform_execute",
                    step_name=step.type,
                    message=f"Missing required key in auth config: '{required_key}'",
                )

        output_key = step.output["auth"]
        state.derived[output_key] = prestodb.auth.BasicAuthentication(
            auth_config["username"], auth_config["password"]
        )


TransformRegistry.register("resolve_presto_auth", ResolvePrestoAuthTransform)
