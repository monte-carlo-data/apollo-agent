from abc import ABC, abstractmethod
from typing import Optional, Dict


class AgentUpdater(ABC):
    """
    Agent updater base abstract class, used for the agent to update itself.
    An integration can optionally set an updater in `agent.updater`.
    """

    @abstractmethod
    def update(
        self,
        platform_info: Optional[Dict],
        image: Optional[str],
        timeout_seconds: Optional[int],
        **kwargs,  # type: ignore
    ) -> Dict:
        """
        Updates the agent to the specified image.
        :param platform_info: the `platform_info` object set in the Agent, some integrations (like GCP) use this to
            avoid having to re-read additional data from the metadata service.
        :param image: optional image id, expected format: montecarlodata/repo_name:tag, for example:
            montecarlodata/agent:1.0.1-cloudrun.
        :param timeout_seconds: optional timeout, the default value is decided by the implementation.
        """
        pass
