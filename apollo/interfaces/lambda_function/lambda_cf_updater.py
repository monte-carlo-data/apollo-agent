import logging
import os
from typing import Optional, Dict, List, cast, Any
from datetime import datetime, timezone
import boto3
from botocore.client import BaseClient
from botocore.exceptions import WaiterError

from apollo.agent.env_vars import CLOUDFORMATION_STACK_ID_ENV_VAR
from apollo.agent.models import AgentUpdateError
from apollo.agent.updater import AgentUpdater

_CF_UPDATE_WAITER = "stack_update_complete"
_STACK_SUCCESSFUL_UPDATE_STATE = "UPDATE_COMPLETE"
_DEFAULT_CAPABILITIES = "CAPABILITY_IAM"

_UPDATE_STACK_WAIT_DELAY = 5
_UPDATE_STACK_WAIT_MAX_ATTEMPTS = 720
_UPDATE_MAX_EVENT_COUNT = 100

_PARAMETER_KEY_ATTR_NAME = "ParameterKey"
_PARAMETER_VALUE_ATTR_NAME = "ParameterValue"
_PARAMETER_USE_PREVIOUS_VALUE_ATTR_NAME = "UsePreviousValue"

_IMAGE_URI_TEMPLATE_PARAMETER_NAME = "ImageUri"

_NEW_PARAMETERS_ARG_NAME = "parameters"
_TEMPLATE_URL_ARG_NAME = "template_url"

logger = logging.getLogger(__name__)


class LambdaCFUpdater(AgentUpdater):
    """
    Agent updater for CloudFormation, it uses `boto3.cloudformation` API to update the stack and get events.
    It requires the env var: `MCD_STACK_ID` to be set with the CF Stack ID.
    """

    def get_current_image(self, platform_info: Optional[Dict]) -> Optional[str]:
        """
        Returns the current value for the "ImageUri" template parameter.
        """
        client = self._get_cloudformation_client()
        return self._get_image_uri_parameter(client=client)

    def get_update_logs(self, start_time: datetime, limit: int) -> List[Dict]:
        """
        Returns the list of CloudFormation events since the specified time, up to `limit` events are returned.
        """
        client = self._get_cloudformation_client()
        stack_id = self._get_stack_id()
        return self._get_stack_events(
            client=client, stack_id=stack_id, start_time=start_time, limit=limit
        )

    def update(
        self,
        platform_info: Optional[Dict],
        image: Optional[str],
        timeout_seconds: Optional[int],
        **kwargs,  # type: ignore
    ) -> Dict:
        """
        Updates the CF Stack, image is expected to have this format:
            <account_number>.dkr.ecr.<region>>.amazonaws.com/<repo_name>>:<image_tag>
        Optional parameters supported through kwargs:
        - 'parameters': a dictionary with new values for the template parameters
        - 'wait_for_completion': a bool indicating if this method should wait for the update to complete,
            defaults to False
        - 'template_url': a new value for "TemplateURL", defaults to None and it triggers the update with
            UsePreviousTemplate=true
        - 'region': optional value to use to force the region component in the image URI, by default it is replaced
            with "*" that will be handled by the template and replaced with the right region.
        """
        client = self._get_cloudformation_client()
        template_url = kwargs.get(_TEMPLATE_URL_ARG_NAME)

        stack_id = self._get_stack_id()
        logger.info(
            f"Update CF stack requested", extra=dict(stack_id=stack_id, image=image)
        )
        # the template takes care of replacing "*" with the right region
        region = kwargs.get("region", "*")
        new_image_uri = self._get_new_image_uri(image, region) if image else None

        parameters = self._merge_parameters(
            client=client,
            stack_id=stack_id,
            new_image_uri=new_image_uri,
            new_parameters=kwargs.get(_NEW_PARAMETERS_ARG_NAME) or {},
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
        start_time = datetime.now(timezone.utc)
        client.update_stack(**update_stack_args)

        if kwargs.get("wait_for_completion", False):
            error_message = self._wait_for_stack_update(
                client=client, stack_id=stack_id
            )
            events = self._get_stack_events(
                client=client,
                stack_id=stack_id,
                start_time=start_time,
                limit=_UPDATE_MAX_EVENT_COUNT,
            )
            status = self._get_stack_status(client=client)
            return {
                "failed": status != _STACK_SUCCESSFUL_UPDATE_STATE,
                "error_message": error_message,
                "status": status,
                "image_uri": self._get_image_uri_parameter(client=client),
                "events": events,
            }
        else:
            status = self._get_stack_status(client=client)
            return {
                "status": status,
            }

    @staticmethod
    def _get_cloudformation_client() -> BaseClient:
        return cast(BaseClient, boto3.client("cloudformation"))

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
    def _wait_for_stack_update(client: BaseClient, stack_id: str) -> Optional[str]:
        """
        Waits for the stack to update, returns `None` if update was successful and the error message if
        it was not.
        """
        try:
            client.get_waiter(_CF_UPDATE_WAITER).wait(
                StackName=stack_id,
                WaiterConfig={
                    "Delay": _UPDATE_STACK_WAIT_DELAY,
                    "MaxAttempts": _UPDATE_STACK_WAIT_MAX_ATTEMPTS,
                },
            )
            return None
        except WaiterError as err:
            return str(err)

    @classmethod
    def _merge_parameters(
        cls,
        client: BaseClient,
        stack_id: str,
        new_image_uri: Optional[str],
        new_parameters: Dict,
    ) -> List[Dict]:
        parameters = cls._get_stack_parameters(client=client)
        for param in parameters:
            param_name = param[_PARAMETER_KEY_ATTR_NAME]
            if param_name == _IMAGE_URI_TEMPLATE_PARAMETER_NAME and new_image_uri:
                logger.info(f"Updating ImageUri to: {new_image_uri}")
                param[_PARAMETER_VALUE_ATTR_NAME] = new_image_uri
            elif param_name in new_parameters:
                new_value = new_parameters[param_name]
                param[_PARAMETER_VALUE_ATTR_NAME] = str(new_value)
            else:
                param[_PARAMETER_USE_PREVIOUS_VALUE_ATTR_NAME] = True
                if _PARAMETER_VALUE_ATTR_NAME in param:
                    param.pop(_PARAMETER_VALUE_ATTR_NAME)

        # just for logging
        parameter_log_values = {
            cast(str, param.get(_PARAMETER_KEY_ATTR_NAME)): (
                param.get(_PARAMETER_VALUE_ATTR_NAME) or "<previous value>"
            )
            for param in parameters
        }
        logger.info(
            f"Updating stack",
            extra=dict(stack_id=stack_id, parameters=parameter_log_values),
        )
        return parameters

    @staticmethod
    def _get_stack_id():
        stack_id = os.getenv(CLOUDFORMATION_STACK_ID_ENV_VAR)
        if not stack_id:
            raise AgentUpdateError(
                f"Missing {CLOUDFORMATION_STACK_ID_ENV_VAR} environment variable"
            )
        return stack_id

    @classmethod
    def _get_stack_details(cls, client: BaseClient) -> Dict:
        return client.describe_stacks(StackName=cls._get_stack_id())

    @classmethod
    def _get_stack_status(cls, client: BaseClient) -> str:
        return cls._get_stack_details(client=client)["Stacks"][0]["StackStatus"]

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

    @classmethod
    def _get_stack_events(
        cls,
        client: BaseClient,
        stack_id: str,
        start_time: datetime,
        limit: int,
    ) -> List[Dict]:
        describe_event_args = {"StackName": stack_id}
        complete = False

        result: List[Dict] = []
        while not complete:
            # events are returned in reverse chronological order
            # we stop when we get an event older than `start_time`
            # describe_stack_events doesn't accept NextToken=None, we add "NextToken" later
            describe_events_response = client.describe_stack_events(
                **describe_event_args
            )
            events = describe_events_response.get("StackEvents")
            for event in events:
                if event.get("Timestamp") < start_time or len(result) == limit:
                    complete = True
                    break
                result.append(cls._build_response_event(event))
            next_token = describe_events_response.get("NextToken")
            if next_token is None:
                complete = True  # no next token, we reached the end of the log
            else:
                describe_event_args["NextToken"] = next_token
        return result

    @staticmethod
    def _build_response_event(event: Dict) -> Dict:
        return {
            "timestamp": event["Timestamp"].isoformat()
            if "Timestamp" in event
            else None,
            "logical_resource_id": event.get("LogicalResourceId"),
            "resource_type": event.get("ResourceType"),
            "resource_status": event.get("ResourceStatus"),
            "resource_status_reason": event.get("ResourceStatusReason"),
        }
