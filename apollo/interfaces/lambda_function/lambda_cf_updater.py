import logging
import os
from typing import Optional, Dict, List, cast, Any

import boto3
from botocore.client import BaseClient

from apollo.agent.env_vars import (
    CLOUD_FORMATION_STACK_ID_ENV_VAR,
    AGENT_WRAPPER_TYPE_ENV_VAR,
    WRAPPER_TYPE_CLOUDFORMATION,
)
from apollo.agent.models import AgentUpdateError
from apollo.agent.updater import AgentUpdater

_CF_UPDATE_WAITER = "stack_update_complete"
_STACK_UPDATE_STATE = "UPDATE_COMPLETE"
_DEFAULT_CAPABILITIES = "CAPABILITY_IAM"
_WAIT_MAX_ATTEMPTS = 720

_PARAMETER_KEY_ATTR_NAME = "ParameterKey"
_PARAMETER_VALUE_ATTR_NAME = "ParameterValue"
_PARAMETER_USE_PREVIOUS_VALUE_ATTR_NAME = "UsePreviousValue"

_IMAGE_URI_TEMPLATE_PARAMETER_NAME = "ImageUri"

_NEW_PARAMETERS_ARG_NAME = "parameters"
_TEMPLATE_URL_ARG_NAME = "template_url"

logger = logging.getLogger(__name__)


class LambdaCFUpdater(AgentUpdater):
    def update(
        self,
        platform_info: Optional[Dict],
        image: Optional[str],
        timeout_seconds: Optional[int],
        **kwargs,  # type: ignore
    ) -> Dict:
        wrapper_type = os.getenv(AGENT_WRAPPER_TYPE_ENV_VAR)
        if wrapper_type != WRAPPER_TYPE_CLOUDFORMATION:
            raise AgentUpdateError("Updates supported only for CloudFormation wrapper")
        client = self._get_cloudformation_client()
        template_url = kwargs.get(_TEMPLATE_URL_ARG_NAME)
        new_parameters = kwargs.get(_NEW_PARAMETERS_ARG_NAME) or {}

        stack_id = self._get_stack_id()
        logger.info(
            f"Update CF stack requested", extra=dict(stack_id=stack_id, image=image)
        )

        parameters = self._get_stack_parameters(client=client)
        for param in parameters:
            param_name = param[_PARAMETER_KEY_ATTR_NAME]
            if param_name == _IMAGE_URI_TEMPLATE_PARAMETER_NAME and image:
                new_image_uri = self._get_new_image_uri(
                    image,
                    kwargs.get(
                        "region", "*"
                    ),  # the template takes care of replacing "*" with the right region
                )
                logger.info(f"Updating ImageUri to: {new_image_uri}")
                param[_PARAMETER_VALUE_ATTR_NAME] = new_image_uri
            elif param_name in new_parameters:
                new_value = new_parameters[param_name]
                param[_PARAMETER_VALUE_ATTR_NAME] = str(new_value)
            else:
                param[_PARAMETER_USE_PREVIOUS_VALUE_ATTR_NAME] = True
                if _PARAMETER_VALUE_ATTR_NAME in param:
                    param.pop(_PARAMETER_VALUE_ATTR_NAME)
        parameter_values = {
            cast(str, param.get(_PARAMETER_KEY_ATTR_NAME)): (
                param.get(_PARAMETER_VALUE_ATTR_NAME) or "<previous value>"
            )
            for param in parameters
        }
        logger.info(
            f"Updating stack", extra=dict(stack_id=stack_id, **parameter_values)
        )

        update_stack_args: Dict[str, Any] = dict(
            StackName=stack_id,
            Parameters=parameters,
            Capabilities=[_DEFAULT_CAPABILITIES],
        )
        if template_url:
            update_stack_args["TemplateURL"] = template_url
        else:
            update_stack_args["UsePreviousTemplate"] = True
        client.update_stack(**update_stack_args)

        client.get_waiter(_CF_UPDATE_WAITER).wait(
            StackName=stack_id,
            WaiterConfig={
                "Delay": 5,
                "MaxAttempts": _WAIT_MAX_ATTEMPTS,
            },
        )
        status = self._get_stack_details(client=client)["Stacks"][0]["StackStatus"]
        if status != _STACK_UPDATE_STATE:
            raise ValueError(f"Update failed, status: {status}")
        return {
            "Status": status,
            "ImageUri": self._get_image_uri_parameter(client=client),
        }

    @staticmethod
    def _get_cloudformation_client() -> BaseClient:
        return cast(BaseClient, boto3.client("cloudformation"))

    def get_current_image(self, platform_info: Optional[Dict]) -> Optional[str]:
        client = self._get_cloudformation_client()
        return self._get_image_uri_parameter(client=client)

    def _get_image_uri_parameter(self, client: BaseClient):
        parameters = self._get_stack_parameters(client=client)
        return next(
            (
                param[_PARAMETER_VALUE_ATTR_NAME]
                for param in parameters
                if param[_PARAMETER_KEY_ATTR_NAME] == _IMAGE_URI_TEMPLATE_PARAMETER_NAME
            ),
            None,
        )

    @staticmethod
    def _get_stack_id():
        stack_id = os.getenv(CLOUD_FORMATION_STACK_ID_ENV_VAR)
        if not stack_id:
            raise AgentUpdateError(
                f"Missing {CLOUD_FORMATION_STACK_ID_ENV_VAR} environment variable"
            )
        return stack_id

    @classmethod
    def _get_stack_details(cls, client: BaseClient) -> Dict:
        return client.describe_stacks(StackName=cls._get_stack_id())

    @classmethod
    def _get_stack_parameters(cls, client: BaseClient) -> List[Dict]:
        return cls._get_stack_details(client=client)["Stacks"][0].get("Parameters")

    @staticmethod
    def _get_new_image_uri(image: str, region: str) -> str:
        domain, repo_tag = image.split("/")
        domain_components = domain.split(".")
        if len(domain_components) > 3:
            domain_components[3] = region
        return f'{".".join(domain_components)}/{repo_tag}'
