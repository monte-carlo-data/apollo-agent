import json
import logging
import os
from datetime import datetime, timezone
from typing import List, Dict, Optional

from azure.mgmt.resource import ResourceManagementClient

from apollo.agent.env_vars import LAST_UPDATE_TS_ENV_VAR
from apollo.agent.models import AgentError
from apollo.agent.updater import AgentUpdater
from apollo.integrations.azure_blob.utils import AzureUtils

logger = logging.getLogger(__name__)

# use this mapping to expose more user-friendly parameter names
_PARAMETERS_ENV_VARS = {
    "WorkerProcessCount": "FUNCTIONS_WORKER_PROCESS_COUNT",
    "ThreadCount": "PYTHON_THREADPOOL_THREAD_COUNT",
    "MaxConcurrentActivities": "AzureFunctionsJobHost__extensions__durableTask__maxConcurrentActivityFunctions",
    "MaxConcurrentOrchestratorFunctions": "AzureFunctionsJobHost__extensions__durableTask__maxConcurrentOrchestratorFunctions",
}

# any other parameter prefixed with "env." will be mapped to an env var, for example
# the parameter env.DEBUG will set DEBUG
_ENV_PREFIX = "env."


class AzureUpdater(AgentUpdater):
    """
    Agent updater implementation for Azure Functions.
    The update operations works by updating the resource using Azure Resource Manager API and setting the new
    value for the "LinuxFxVersion" property that is expected to be "DOCKER|docker.io/org/repo/image:tag".
    Updating environment variables is also supported by specifying parameters when running the update operation,
    the default parameters: WorkerProcessCount, ThreadCount and MaxConcurrentActivities are mapped to the
    corresponding environment variables, any other parameter prefixed with "env." will set the corresponding
    environment variable, for example "env.MCD_DEBUG" parameter will set "MCD_DEBUG" env var.
    """

    def update(
        self,
        image: Optional[str],
        timeout_seconds: Optional[int],
        parameters: Optional[Dict] = None,
        **kwargs,  # type: ignore
    ) -> Dict:
        update_args = {
            "image": image,
            "parameters": parameters,
        }
        logger.info("Update requested", extra=update_args)
        if not image and not parameters:
            raise AgentError("Either image or parameters must be provided")

        client = self._get_resource_management_client()
        if image:
            update_image_parameters = {
                "properties": {"siteConfig": {"linuxFxVersion": f"DOCKER|{image}"}}
            }
            serialized_parameters = json.dumps(update_image_parameters).encode("utf-8")

            client.resources.begin_update(
                **self._get_function_resource_args(),
                parameters=serialized_parameters,  # type: ignore
            )
        update_appsettings_parameters = {
            "properties": self._get_update_env_vars(parameters or {})
        }
        serialized_parameters = json.dumps(update_appsettings_parameters).encode(
            "utf-8"
        )

        # to update env vars we need to update <function_name>/config/appsettings
        client.resources.begin_update(
            **self._get_function_resource_args("/config/appsettings"),
            parameters=serialized_parameters,  # type: ignore
        )

        logger.info("Update triggered", extra=update_args)
        update_args_list = [
            f"{key}: {value}" for key, value in update_args.items() if value
        ]
        return {"message": f"Update in progress, {', '.join(update_args_list)}"}

    def get_current_image(self) -> Optional[str]:
        try:
            resource = self.get_function_resource()
            return (
                resource.get("properties", {})
                .get("siteConfig", {})
                .get("linuxFxVersion")
            )
        except Exception as exc:
            logger.error(f"Unable to get current image: {exc}")
            return None

    def get_update_logs(self, start_time: datetime, limit: int) -> List[Dict]:
        # no support for update logs in Azure
        return []

    @classmethod
    def get_function_resource(cls) -> Dict:
        client = cls._get_resource_management_client()
        resource = client.resources.get(**cls._get_function_resource_args())
        return dict(resource.as_dict())

    @classmethod
    def get_current_parameter_values(cls) -> Dict:
        return {
            param_name: os.getenv(env_var)
            for param_name, env_var in _PARAMETERS_ENV_VARS.items()
        }

    @staticmethod
    def _get_resource_management_client() -> ResourceManagementClient:
        return ResourceManagementClient(
            AzureUtils.get_default_credential(), AzureUtils.get_subscription_id()
        )

    @staticmethod
    def _get_function_resource_args(sub_path: str = "") -> Dict:
        resource_group = AzureUtils.get_resource_group()
        function_name = AzureUtils.get_function_name()
        return dict(
            resource_group_name=resource_group,
            resource_provider_namespace="Microsoft.Web",
            parent_resource_path="sites",
            resource_type="",
            resource_name=f"{function_name}{sub_path}",
            api_version="2022-03-01",
        )

    @staticmethod
    def _get_update_env_vars(parameters: Dict) -> Dict:
        env_vars = {
            env_var: str(parameters[param_name])
            for param_name, env_var in _PARAMETERS_ENV_VARS.items()
            if param_name in parameters
        }
        env_vars.update(
            {
                key[len(_ENV_PREFIX) :]: value
                for key, value in parameters.items()
                if key.startswith(_ENV_PREFIX)
            }
        )
        env_vars[LAST_UPDATE_TS_ENV_VAR] = datetime.now(timezone.utc).isoformat()
        logger.info(f"Updating env vars: {env_vars}")
        return env_vars
