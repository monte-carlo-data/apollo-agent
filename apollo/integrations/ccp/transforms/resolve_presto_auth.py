import prestodb

from apollo.integrations.ccp.errors import CcpPipelineError
from apollo.integrations.ccp.models import PipelineState, TransformStep
from apollo.integrations.ccp.template import TemplateEngine
from apollo.integrations.ccp.transforms.base import Transform
from apollo.integrations.ccp.transforms.registry import TransformRegistry


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

    def execute(self, step: TransformStep, state: PipelineState) -> None:
        if "auth" not in step.input:
            raise CcpPipelineError(
                stage="transform_input",
                step_name=step.type,
                message="'auth' key required in step input",
            )

        auth_config = TemplateEngine.render(step.input["auth"], state)

        if not auth_config:
            raise CcpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message="'auth' input resolved to empty — check the 'when' guard",
            )

        if not isinstance(auth_config, dict):
            raise CcpPipelineError(
                stage="transform_execute",
                step_name=step.type,
                message=f"'auth' must be a dict, got {type(auth_config).__name__}",
            )

        for required_key in ("username", "password"):
            if required_key not in auth_config:
                raise CcpPipelineError(
                    stage="transform_execute",
                    step_name=step.type,
                    message=f"Missing required key in auth config: '{required_key}'",
                )

        output_key = step.output.get("auth")
        if not output_key:
            raise CcpPipelineError(
                stage="transform_output",
                step_name=step.type,
                message="'auth' output key required",
            )

        state.derived[output_key] = prestodb.auth.BasicAuthentication(
            auth_config["username"], auth_config["password"]
        )


TransformRegistry.register("resolve_presto_auth", ResolvePrestoAuthTransform)
