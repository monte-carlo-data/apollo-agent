from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple

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

    @abstractmethod
    def get_infra_details(self) -> Dict:
        """
        Get infrastructure information like the CloudFormation template or current parameters.
        """
        pass

    def pre_health_check(self, headers: Any) -> Optional[Tuple[Dict, int]]:
        """Called before health logic runs.

        Return a (body, status_code) tuple to short-circuit the health
        endpoint, or None to proceed with the normal health response.
        """
        return None

    def post_health_check(self, health_dict: Dict) -> Optional[Tuple[Dict, int]]:
        """Called after the health dict is built.

        Return a (body, status_code) tuple to override the response,
        or None for the default 200.  The hook may mutate *health_dict*
        in place (e.g. to add error details).
        """
        return None
