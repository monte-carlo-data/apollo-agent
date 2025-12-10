from abc import ABC, abstractmethod
from typing import Optional, Any, Dict

from apollo.common.agent.models import AgentOperation
from apollo.common.agent.redact import AgentRedactUtilities

# any lower cased attribute including any of these values in the key will be redacted in log messages
_REDACTED_ATTRIBUTES = [
    "pass",
    "secret",
    "client",
    "token",
    "user",
    "auth",
    "credential",
    "key",
]


class BaseProxyClient(ABC):
    @property
    @abstractmethod
    def wrapped_client(self):
        pass

    def close(self):
        """
        Closes the underlying client if needed.
        """
        pass

    def get_error_type(self, error: Exception) -> Optional[str]:
        """
        Returns the error type to send to the client based on the given exception, this is optional but
        in some cases is used to map to the right exception type client side.
        :param error: the exception to return the error type for
        :return: the error type to send to the client or None if there's no specific error type for this exception.
        """
        return None

    def get_error_extra_attributes(self, error: Exception) -> Optional[Dict]:
        """
        Returns an additional set of attributes that might be needed client side to create the error object.
        :param error: the exception to return the error attributes for
        :return: a dictionary with attributes like error_code for example.
        """
        return None

    def log_payload(self, request: AgentOperation) -> Dict:
        """
        Returns the `extra` payload to include in the log message for the given operation on this client.
        :param operation: the operation that is about to be executed on this client
        :return: The `extra` payload to include in the log message, by default `operation.to_dict()` but sub classes
            can override to return more or less data (for example to trim/redact sensitive data).
        """
        extra = request.to_dict()

        # we're already logging trace_id as mcd_trace_id, avoid the duplicated attribute
        extra.pop("trace_id", None)

        return AgentRedactUtilities.redact_attributes(extra, _REDACTED_ATTRIBUTES)

    def process_result(self, value: Any) -> Any:
        """
        Process the result before sending it to the client, it allows the client to convert objects before
        JSON serialization takes place, for example Looker client uses it to convert Looker API objects
        into dictionaries
        """
        return value

    def should_log_exception(self, ex: Exception) -> bool:
        """
        It can be used to prevent logging an error for certain exceptions, for example storage client is not logging
        not found errors. By default, all exceptions are logged.
        :param ex: the exception occurred.
        :return: True if the exception should be logged, False otherwise. By default, True is returned.
        """
        return True
