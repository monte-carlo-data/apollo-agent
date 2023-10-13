from abc import ABC, abstractmethod
from typing import Optional, Dict


class AgentUpdater(ABC):
    """
    Agent updater base abstract class, used for the agent to update itself.
    An integration can optionally set an updater in `agent.updater`.
    """

    @abstractmethod
    def update(self, platform_info: Optional[Dict], timeout_seconds: Optional[int], **kwargs) -> Dict:  # type: ignore
        pass
