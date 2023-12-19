import os
from typing import Dict, Optional

from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient

from apollo.agent.constants import PLATFORM_AZURE
from apollo.agent.platform import AgentPlatformProvider
from apollo.agent.updater import AgentUpdater
from apollo.interfaces.azure.azure_updater import AzureUpdater


class AzurePlatformProvider(AgentPlatformProvider):
    @property
    def platform(self) -> str:
        return PLATFORM_AZURE

    @property
    def platform_info(self) -> Dict:
        return {}

    @property
    def updater(self) -> Optional[AgentUpdater]:
        return AzureUpdater()

    def get_infra_details(self) -> Dict:
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

        return {
            "resource": resource.as_dict(),
        }
