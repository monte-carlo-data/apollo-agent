from abc import ABC, abstractmethod
from typing import Dict


class AgentLogContext(ABC):
    """
    Base abstract class representing an object where the agent sets the log context for the current operation.
    Before an operation is executed the agent will call `set_agent_context` with information to be included in log
    messages like trace id and operation name.
    How this information is used is platform dependent, for example GCP stores it and then uses it in `logging.Filter`
    implementation.
    """

    @abstractmethod
    def set_agent_context(self, context: Dict):
        """
        Set the context information for the current operation.
        :param context: a dictionary including information that should be logged with each request.
        """
        pass
