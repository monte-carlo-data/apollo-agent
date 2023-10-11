from abc import ABC, abstractmethod
from typing import Optional, Dict

from apollo.agent.models import AgentOperation


class BaseProxyClient(ABC):
    @property
    @abstractmethod
    def wrapped_client(self):
        pass

    def get_error_type(self, error: Exception) -> Optional[str]:
        """
        Returns the error type to send to the client based on the given exception, this is optional but
        in some cases is used to map to the right exception type client side.
        :param error: the exception to return the error type for
        :return: the error type to send to the client or None if there's no specific error type for this exception.
        """
        return None

    def log_payload(self, operation: AgentOperation) -> Dict:
        """
        Returns the `extra` payload to include in the log message for the given operation on this client.
        :param operation: the operation that is about to be executed on this client
        :return: The `extra` payload to include in the log message, by default `operation.to_dict()` but sub classes
            can override to return more or less data (for example to trim/redact sensitive data).
        """
        return operation.to_dict()
