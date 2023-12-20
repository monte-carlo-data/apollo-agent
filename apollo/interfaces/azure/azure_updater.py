from datetime import datetime
from typing import List, Dict, Optional

from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient

from apollo.agent.updater import AgentUpdater


class AzureUpdater(AgentUpdater):
    def update(
        self, image: Optional[str], timeout_seconds: Optional[int], **kwargs  # type: ignore
    ) -> Dict:
        return {}

    def get_current_image(self) -> Optional[str]:
        resource = self.get_resource()
        return (
            resource.get("properties", {}).get("siteConfig", {}).get("linuxFxVersion")
        )

    def get_update_logs(self, start_time: datetime, limit: int) -> List[Dict]:
        return []

    @staticmethod
    def get_resource() -> Dict:
        client = ResourceManagementClient(
            DefaultAzureCredential(), "a8874c8e-8e44-44c2-8e3d-8ff3174cac1d"
        )
        resource = client.resources.get(
            resource_group_name="mrostan-dev-agent-rg",
            resource_provider_namespace="Microsoft.Web",
            parent_resource_path="sites",
            resource_type="",
            resource_name="mrostan-dev-agent",
            api_version="2022-03-01",
        )
        return dict(resource.as_dict())
