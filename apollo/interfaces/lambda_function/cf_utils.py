import os
from typing import Dict, cast, List

import boto3
from botocore.client import BaseClient

from apollo.agent.env_vars import CLOUDFORMATION_STACK_ID_ENV_VAR
from apollo.agent.models import AgentConfigurationError


class CloudFormationUtils:
    """
    Utilities class to work with CloudFormation using a boto3 client.
    Gets the stack id from `MCD_STACK_ID` environment variable.
    """

    @staticmethod
    def get_cloudformation_client(**kwargs) -> BaseClient:  # type: ignore
        return cast(BaseClient, boto3.client("cloudformation", **kwargs))

    @staticmethod
    def get_stack_id() -> str:
        stack_id = os.getenv(CLOUDFORMATION_STACK_ID_ENV_VAR)
        if not stack_id:
            raise AgentConfigurationError(
                f"Missing {CLOUDFORMATION_STACK_ID_ENV_VAR} environment variable"
            )
        return stack_id

    @classmethod
    def get_stack_details(cls, client: BaseClient) -> Dict:
        return client.describe_stacks(StackName=cls.get_stack_id())

    @classmethod
    def get_stack_parameters(cls, client: BaseClient) -> List[Dict]:
        return cls.get_stack_details(client=client)["Stacks"][0].get("Parameters")

    @classmethod
    def get_stack_status(cls, client: BaseClient) -> str:
        return cls.get_stack_details(client=client)["Stacks"][0]["StackStatus"]

    @classmethod
    def get_infra_details(cls) -> Dict:
        client = cls.get_cloudformation_client()
        stack_id = cls.get_stack_id()

        template = client.get_template(StackName=stack_id).get("TemplateBody")
        parameters = cls.get_stack_parameters(client)
        return {
            "template": template,
            "parameters": parameters,
        }
