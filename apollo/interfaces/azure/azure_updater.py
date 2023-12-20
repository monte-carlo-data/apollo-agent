import json
import logging
import os
from datetime import datetime
from io import StringIO
from typing import List, Dict, Optional, cast

from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient

from apollo.agent.models import AgentError
from apollo.agent.updater import AgentUpdater

logger = logging.getLogger(__name__)


class AzureUpdater(AgentUpdater):
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
        serialized_parameters = StringIO(json.dumps(parameters))

        client = self._get_resource_management_client()
        client.resources.begin_update(
            **self._get_resource_args(), parameters=serialized_parameters  # type: ignore
        )
        return {"message": f"Update in progress, image: {image}"}

    def get_current_image(self) -> Optional[str]:
        resource = self.get_resource()
        return (
            resource.get("properties", {}).get("siteConfig", {}).get("linuxFxVersion")
        )

    def get_update_logs(self, start_time: datetime, limit: int) -> List[Dict]:
        return []

    @classmethod
    def get_resource(cls) -> Dict:
        client = cls._get_resource_management_client()
        resource = client.resources.get(**cls._get_resource_args())
        return dict(resource.as_dict())

    @staticmethod
    def _get_resource_management_client() -> ResourceManagementClient:
        owner_name = cast(
            str, os.getenv("WEBSITE_OWNER_NAME")
        )  # subscription_id+resource_group_region_etc
        subscription_id = owner_name.split("+")[0]

        # this code requires AZURE_CLIENT_ID to be set if a user managed identity is used
        return ResourceManagementClient(DefaultAzureCredential(), subscription_id)

    @staticmethod
    def _get_resource_args() -> Dict:
        resource_group = os.getenv("WEBSITE_RESOURCE_GROUP")
        function_name = os.getenv("WEBSITE_SITE_NAME")
        return dict(
            resource_group_name=resource_group,
            resource_provider_namespace="Microsoft.Web",
            parent_resource_path="sites",
            resource_type="",
            resource_name=function_name,
            api_version="2022-03-01",
        )
