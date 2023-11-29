import os
from typing import Dict, cast, List

import boto3
from botocore.client import BaseClient

from apollo.agent.env_vars import CLOUDFORMATION_STACK_ID_ENV_VAR
from apollo.agent.models import AgentConfigurationError


class CloudFormationUtils:
    @staticmethod
    def get_cloudformation_client() -> BaseClient:
        return cast(BaseClient, boto3.client("cloudformation"))

    @staticmethod
    def get_stack_id():
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
