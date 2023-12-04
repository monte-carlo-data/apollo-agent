from abc import ABC, abstractmethod
from typing import Dict, Optional

from apollo.agent.updater import AgentUpdater


class AgentPlatformProvider(ABC):
    @property
    @abstractmethod
    def platform(self) -> str:
        pass

    @property
    @abstractmethod
    def platform_info(self) -> Dict:
        pass

    @property
    @abstractmethod
    def updater(self) -> Optional[AgentUpdater]:
        pass

    @abstractmethod
    def get_infra_details(self) -> Dict:
        pass
