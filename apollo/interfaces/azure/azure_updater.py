import json
import logging
from datetime import datetime
from typing import List, Dict, Optional

from azure.mgmt.resource import ResourceManagementClient

from apollo.agent.models import AgentError
from apollo.agent.updater import AgentUpdater
from apollo.integrations.azure_blob.utils import AzureUtils

logger = logging.getLogger(__name__)


class AzureUpdater(AgentUpdater):
    """
    Agent updater implementation for Azure Functions.
    The update operations works by updating the resource using Azure Resource Manager API and setting the new
    value for the "LinuxFxVersion" property that is expected to be "DOCKER|docker.io/org/repo/image:tag".
    """

    def update(
        self, image: Optional[str], timeout_seconds: Optional[int], **kwargs  # type: ignore
    ) -> Dict:
        logger.info(
            "Update requested",
            extra={
                "image": image,
            },
        )
        if not image:
            raise AgentError("Image parameter is required")

        parameters = {
            "properties": {"siteConfig": {"linuxFxVersion": f"DOCKER|{image}"}}
        }
        serialized_parameters = json.dumps(parameters).encode("utf-8")

        client = self._get_resource_management_client()
        client.resources.begin_update(
            **self._get_function_resource_args(), parameters=serialized_parameters  # type: ignore
        )
        logger.info("Update complete", extra={"image": image})
        return {"message": f"Update in progress, image: {image}"}

    def get_current_image(self) -> Optional[str]:
        resource = self.get_function_resource()
        return (
            resource.get("properties", {}).get("siteConfig", {}).get("linuxFxVersion")
        )

    def get_update_logs(self, start_time: datetime, limit: int) -> List[Dict]:
        # no support for update logs in Azure
        return []

    @classmethod
    def get_function_resource(cls) -> Dict:
        client = cls._get_resource_management_client()
        resource = client.resources.get(**cls._get_function_resource_args())
        return dict(resource.as_dict())

    @staticmethod
    def _get_resource_management_client() -> ResourceManagementClient:
        return ResourceManagementClient(
            AzureUtils.get_default_credential(), AzureUtils.get_subscription_id()
        )

    @staticmethod
    def _get_function_resource_args() -> Dict:
        resource_group = AzureUtils.get_resource_group()
        function_name = AzureUtils.get_function_name()
        return dict(
            resource_group_name=resource_group,
            resource_provider_namespace="Microsoft.Web",
            parent_resource_path="sites",
            resource_type="",
            resource_name=function_name,
            api_version="2022-03-01",
        )
