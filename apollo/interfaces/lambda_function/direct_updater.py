import logging
import os
from datetime import datetime
from typing import List, Dict, Optional, cast

import boto3
from botocore.client import BaseClient
from botocore.exceptions import WaiterError

from apollo.agent.env_vars import AWS_LAMBDA_FUNCTION_NAME_ENV_VAR
from apollo.agent.models import AgentConfigurationError
from apollo.agent.updater import AgentUpdater

logger = logging.getLogger(__name__)

_LAMBDA_UPDATE_WAITER = "function_updated_v2"
_UPDATE_LAMBDA_WAIT_DELAY = 5
_UPDATE_LAMBDA_WAIT_MAX_ATTEMPTS = 720


class LambdaDirectUpdater(AgentUpdater):
    """
    Agent updater for Lambda direct updates, used for Terraform templates, it uses `boto3.lambda` API to
    update the function code and parameters like MemorySize and ConcurrentExecutions.
    It gets the name of the lambda function from `AWS_LAMBDA_FUNCTION_NAME` env var that is automatically set by
    the Lambda runtime.
    """

    def update(
        self,
        image: Optional[str],
        timeout_seconds: Optional[int],
        parameters: Optional[Dict] = None,
        wait_for_completion: bool = False,
        **kwargs,  # type: ignore
    ) -> Dict:
        """
        Updates the function code to the specified image, the following parameters are supported and will update
        the associated setting in the function: `MemorySize` and `ConcurrentExecutions`.
        :param image: The new image URI to use, if not specified the ImageUri attribute won't be updated, "*" will
            be replaced with the current region.
        :param timeout_seconds: Ignored by this updater
        :param parameters: An optional dictionary specifying new values for the following supported parameters:
            "MemorySize" and "ConcurrentExecutions".
        :param wait_for_completion: If `true` this code will wait for the update to complete using a
            `function_updated_v2` waiter, defaults to False.
        """
        function_name = self._get_function_name()
        parameters = parameters or {}
        memory_size = parameters.get("MemorySize")
        concurrent_executions = parameters.get("ConcurrentExecutions")
        logger.info(
            "Direct update requested",
            extra=dict(
                image=image,
                memory_size=memory_size,
                concurrent_executions=concurrent_executions,
            ),
        )

        client = self._get_lambda_client()
        if image:
            prev_image = self.get_current_image()
            image_uri = image.replace("*", self._get_region())
            if image_uri == prev_image:
                logger.info(
                    "Direct update ignored, no change in image_uri",
                    extra=dict(prev_image=prev_image, image=image),
                )
                result = {}
            else:
                update_result = client.update_function_code(
                    FunctionName=function_name,
                    ImageUri=image_uri,
                    Publish=True,
                )
                if wait_for_completion:
                    error_message = self._wait_for_lambda_update(
                        client=client, function_name=function_name
                    )
                    result = client.get_function(FunctionName=function_name)
                    update_result = result.get("Configuration", {})
                    update_result["ImageUri"] = result.get("Code", {}).get("ImageUri")
                    if error_message:
                        update_result["ErrorMessage"] = error_message

                keys = [
                    "Version",
                    "LastModified",
                    "State",
                    "StateReason",
                    "StateReasonCode",
                    "LastUpdateStatus",
                    "LastUpdateStatusReason",
                    "LastUpdateStatusReasonCode",
                    "ErrorMessage",
                    "ImageUri",
                ]
                result = {
                    key: update_result[key] for key in keys if key in update_result
                }
        else:
            result = {}

        if memory_size:
            logger.info("Updating Lambda Memory Size", dict(memory_size=memory_size))
            client.update_function_configuration(
                FunctionName=function_name, MemorySize=int(memory_size)
            )
            result["MemorySize"] = memory_size

        if concurrent_executions:
            logger.info(
                "Updating Lambda Concurrent Executions",
                dict(concurrent_executions=concurrent_executions),
            )
            client.put_function_concurrency(
                FunctionName=function_name,
                ReservedConcurrentExecutions=int(concurrent_executions),
            )
            result["ConcurrentExecutions"] = concurrent_executions
        return result

    def get_current_image(self) -> Optional[str]:
        """
        Returns the current value for `ImageUri` in the function.
        """
        client = self._get_lambda_client()
        function = client.get_function(FunctionName=self._get_function_name())
        return function.get("Code", {}).get("ImageUri")

    def get_update_logs(self, start_time: datetime, limit: int) -> List[Dict]:
        return []

    @classmethod
    def get_infra_details(cls) -> Dict:
        """
        Returns a dictionary containing a `parameters` entry with the current values for `MemorySize` and
        `ConcurrentExecutions`.
        """
        client = cls._get_lambda_client()
        function = client.get_function(FunctionName=cls._get_function_name())
        configuration = function.get("Configuration", {})
        concurrency = function.get("Concurrency", {})
        return {
            "template": "",
            "parameters": [
                {
                    "ParameterKey": "MemorySize",
                    "ParameterValue": configuration.get("MemorySize"),
                },
                {
                    "ParameterKey": "ConcurrentExecutions",
                    "ParameterValue": concurrency.get("ReservedConcurrentExecutions"),
                },
            ],
        }

    @staticmethod
    def _get_function_name() -> str:
        function_name = os.getenv(AWS_LAMBDA_FUNCTION_NAME_ENV_VAR)
        if not function_name:
            raise AgentConfigurationError(
                f"Missing {AWS_LAMBDA_FUNCTION_NAME_ENV_VAR} environment variable"
            )
        return function_name

    @staticmethod
    def _get_region() -> str:
        return os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", ""))

    @staticmethod
    def _get_lambda_client() -> BaseClient:
        return cast(BaseClient, boto3.client("lambda"))

    @staticmethod
    def _wait_for_lambda_update(
        client: BaseClient, function_name: str
    ) -> Optional[str]:
        """
        Waits for the function to update, returns `None` if update was successful and the error message if
        it was not.
        """
        try:
            client.get_waiter(_LAMBDA_UPDATE_WAITER).wait(
                FunctionName=function_name,
                WaiterConfig={
                    "Delay": _UPDATE_LAMBDA_WAIT_DELAY,
                    "MaxAttempts": _UPDATE_LAMBDA_WAIT_MAX_ATTEMPTS,
                },
            )
            return None
        except WaiterError as err:
            return str(err)
