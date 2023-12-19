import os
from typing import Dict, Optional

from apollo.agent.constants import PLATFORM_AZURE
from apollo.agent.platform import AgentPlatformProvider
from apollo.agent.updater import AgentUpdater


class AzurePlatformProvider(AgentPlatformProvider):
    @property
    def platform(self) -> str:
        return PLATFORM_AZURE

    @property
    def platform_info(self) -> Dict:
        return {}

    @property
    def updater(self) -> Optional[AgentUpdater]:
        return None

    def get_infra_details(self) -> Dict:
        return {}
