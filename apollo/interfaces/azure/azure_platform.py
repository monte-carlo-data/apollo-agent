import os
from typing import Dict, Optional

import requests

from apollo.agent.constants import PLATFORM_AZURE
from apollo.agent.platform import AgentPlatformProvider
from apollo.agent.updater import AgentUpdater


class AzurePlatformProvider(AgentPlatformProvider):
    @property
    def platform(self) -> str:
        return PLATFORM_AZURE

    @property
    def platform_info(self) -> Dict:
        response = requests.get(
            "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com/",
            headers={"Metadata": "true"},
        )
        return {"token": response.text}

    @property
    def updater(self) -> Optional[AgentUpdater]:
        return None

    def get_infra_details(self) -> Dict:
        return {}
