from typing import Dict, Optional

from apollo.agent.agent_platform import AgentPlatformProvider
from apollo.agent.updater import AgentUpdater


class TestPlatformProvider(AgentPlatformProvider):
    def __init__(self, platform: str, platform_info: Optional[Dict] = None):
        self._platform = platform
        self._platform_info = platform_info or {}

    @property
    def platform(self) -> str:
        return self._platform

    @property
    def platform_info(self) -> Dict:
        return self._platform_info

    @property
    def updater(self) -> Optional[AgentUpdater]:
        return None

    def get_infra_details(self) -> Dict:
        return {}
