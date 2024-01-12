from abc import ABC, abstractmethod
from typing import Dict, Optional

from apollo.agent.updater import AgentUpdater


class AgentPlatformProvider(ABC):
    """
    Agent platform provider base abstract class, used for the agent to get platform specific data and
    run platform specific operations.
    An integration can set the provider in `agent.platform_provider`.
    """

    @property
    @abstractmethod
    def platform(self) -> str:
        """
        Returns the platform ID, for example: "AWS" or "GCP"
        """
        pass

    @property
    @abstractmethod
    def platform_info(self) -> Dict:
        """
        Returns a dictionary with platform specific information, returned by the health endpoint.
        """
        pass

    @property
    @abstractmethod
    def updater(self) -> Optional[AgentUpdater]:
        """
        Returns the updater for this platform, if updates are supported or None if updates are not supported.
        """
        pass

    @property
    def client_cache_supported(self) -> bool:
        """
        Returns True if clients can be cached, platforms using multiple threads should return False.
        """
        return True

    @abstractmethod
    def get_infra_details(self) -> Dict:
        """
        Get infrastructure information like the CloudFormation template or current parameters.
        """
        pass
