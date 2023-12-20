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
        return {
            "resource": AzureUpdater.get_resource(),
        }
