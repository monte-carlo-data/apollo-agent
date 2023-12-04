from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Dict, List


class AgentUpdater(ABC):
    """
    Agent updater base abstract class, used for the agent to update itself.
    An integration can optionally set an updater in `agent.updater`.
    """

    @abstractmethod
    def update(
        self,
        image: Optional[str],
        timeout_seconds: Optional[int],
        **kwargs,  # type: ignore
    ) -> Dict:
        """
        Updates the agent to the specified image.
        :param image: optional image id, expected format: montecarlodata/repo_name:tag, for example:
            montecarlodata/agent:1.0.1-cloudrun.
        :param timeout_seconds: optional timeout, the default value is decided by the implementation.
        """
        pass

    @abstractmethod
    def get_current_image(self) -> Optional[str]:
        """
        Returns the image currently used by this service, used by the `health` endpoint.
        """
        pass

    @abstractmethod
    def get_update_logs(self, start_time: datetime, limit: int) -> List[Dict]:
        pass
