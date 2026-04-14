from typing import Optional

import boto3

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry


class ResolveRedshiftCredentialsTransform(Transform):
    """
    Resolves temporary Redshift credentials for a federated IAM user via
    ``redshift:GetClusterCredentials``.

    Calls the Redshift API with the supplied cluster / database parameters and
    stores the short-lived ``DbUser`` and ``DbPassword`` values in ``state.derived``
    under the keys named by ``step.output``.

    Input keys:
      - ``cluster_identifier``: Redshift cluster identifier (required)
      - ``db_user``:            IAM user to map to a Redshift user (required)
      - ``db_name``:            Database name to connect to (required)
      - ``aws_region``:         AWS region where the cluster lives (required)
      - ``assumable_role``:     ARN of an IAM role to assume before calling the API (optional)
      - ``external_id``:        External ID for role assumption (optional)
      - ``duration_seconds``:   Credential lifetime in seconds, 900–3600 (optional; default 900)

    Output keys:
      - ``user``:     key in ``state.derived`` where the resolved ``DbUser`` string is stored
      - ``password``: key in ``state.derived`` where the resolved ``DbPassword`` string is stored
    """

    _REQUIRED_INPUT = ("cluster_identifier", "db_user", "db_name", "aws_region")
    _REQUIRED_OUTPUT = ("user", "password")

    def execute(self, step: TransformStep, state: PipelineState) -> None:
        for key in self._REQUIRED_INPUT:
            if key not in step.input:
                raise CtpPipelineError(
                    stage="transform_input",
                    step_name=step.type,
                    message=f"'{key}' is required in resolve_redshift_credentials input",
                )
        for key in self._REQUIRED_OUTPUT:
            if key not in step.output:
                raise CtpPipelineError(
                    stage="transform_output",
                    step_name=step.type,
                    message=f"'{key}' is required in resolve_redshift_credentials output",
                )

        cluster_identifier = TemplateEngine.render(
            step.input["cluster_identifier"], state
        )
        db_user = TemplateEngine.render(step.input["db_user"], state)
        db_name = TemplateEngine.render(step.input["db_name"], state)
        aws_region = TemplateEngine.render(step.input["aws_region"], state)

        assumable_role = TemplateEngine.render(
            step.input.get("assumable_role", "{{ none }}"), state
        )
        external_id = TemplateEngine.render(
            step.input.get("external_id", "{{ none }}"), state
        )
        duration_seconds = TemplateEngine.render(
            step.input.get("duration_seconds", "{{ none }}"), state
        )

        if duration_seconds is not None:
            try:
                duration_seconds = int(duration_seconds)
            except (ValueError, TypeError) as exc:
                raise CtpPipelineError(
                    stage="transform_input",
                    step_name=step.type,
                    message=f"'duration_seconds' must be an integer, got: {duration_seconds!r}",
                ) from exc

        db_user_out, db_password_out = self._get_cluster_credentials(
            cluster_identifier=cluster_identifier,
            db_user=db_user,
            db_name=db_name,
            aws_region=aws_region,
            assumable_role=assumable_role,
            external_id=external_id,
            duration_seconds=duration_seconds,
            step_name=step.type,
        )

        state.derived[step.output["user"]] = db_user_out
        state.derived[step.output["password"]] = db_password_out

    @staticmethod
    def _get_cluster_credentials(
        cluster_identifier: str,
        db_user: str,
        db_name: str,
        aws_region: str,
        assumable_role: Optional[str],
        external_id: Optional[str],
        duration_seconds: Optional[int],
        step_name: str,
    ) -> tuple[str, str]:
        redshift_client = _create_redshift_client(
            aws_region=aws_region,
            assumable_role=assumable_role,
            external_id=external_id,
        )

        params: dict = {
            "DbUser": db_user,
            "DbName": db_name,
            "ClusterIdentifier": cluster_identifier,
        }
        if duration_seconds is not None:
            params["DurationSeconds"] = duration_seconds

        try:
            response = redshift_client.get_cluster_credentials(**params)
        except Exception as exc:
            raise CtpPipelineError(
                stage="transform_execute",
                step_name=step_name,
                message=f"Failed to get Redshift cluster credentials: {exc}",
            ) from exc

        return response["DbUser"], response["DbPassword"]


def _create_redshift_client(
    aws_region: str,
    assumable_role: Optional[str],
    external_id: Optional[str],
):
    """Create a boto3 Redshift client, optionally assuming an IAM role first."""
    if assumable_role:
        import time

        from apollo.agent.utils import AgentUtils

        session_name = f"mcd_{AgentUtils.generate_random_str(rand_len=5)}_{time.time()}"
        assume_role_params: dict = {
            "RoleArn": assumable_role,
            "RoleSessionName": session_name,
        }
        if external_id:
            assume_role_params["ExternalId"] = external_id

        assumed = boto3.client("sts").assume_role(**assume_role_params)
        creds = assumed["Credentials"]
        session = boto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
            region_name=aws_region,
        )
    else:
        session = boto3.Session(region_name=aws_region)

    return session.client("redshift")


TransformRegistry.register(
    "resolve_redshift_credentials", ResolveRedshiftCredentialsTransform
)
