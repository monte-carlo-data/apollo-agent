import logging
import os
from abc import ABC, abstractmethod
from typing import Iterable, List, Optional, Any, Dict

from apollo.common.agent.models import AgentOperation
from apollo.common.agent.redact import AgentRedactUtilities

logger = logging.getLogger(__name__)

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

    def register_temp_files(self, paths: Iterable[str]) -> None:
        """
        Register filesystem paths (e.g. TLS cert/key/ini files materialized by
        the CTP pipeline) that must be deleted when this client is closed.

        The pipeline that creates these files runs before the client exists and
        has no handle to it, so the factory threads the paths here after
        construction. May be called more than once; paths accumulate.
        """
        existing: List[str] = getattr(self, "_temp_files", [])
        self._temp_files = existing + [p for p in paths if p]

    def close(self):
        """
        Public teardown entry point. Do NOT override this — override
        :meth:`_close_client` instead.

        Runs the subclass teardown and then removes any temp credential files
        registered via :meth:`register_temp_files`, in a ``finally`` so the
        files are deleted even if closing the underlying client raises.
        Otherwise a failure tearing down the connection would leave downloaded
        TLS private keys lingering for the lifetime of the container.
        """
        try:
            self._close_client()
        finally:
            self._remove_temp_files()

    def _close_client(self):
        """
        Release the underlying client/connection if needed. Override in
        subclasses that hold a connection; the base implementation is a no-op.
        """
        pass

    def _remove_temp_files(self) -> None:
        # getattr default guards subclasses whose __init__ never called
        # register_temp_files (e.g. clients constructed outside the factory).
        removed = 0
        for path in getattr(self, "_temp_files", []):
            try:
                os.unlink(path)
                removed += 1
            except FileNotFoundError:
                # Already gone — e.g. a double close, or the same path registered
                # more than once on this client; treat removal as idempotent.
                pass
            except OSError:
                logger.warning("Failed to remove temp credential file", exc_info=True)
        if removed:
            # Positive signal that credential temp files were actually deleted on
            # close (count of real unlinks — the mkstemp names carry no useful info).
            logger.info(
                "Removed %d registered temp credential file(s) on client close",
                removed,
            )
        self._temp_files = []

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

    def get_connection_metadata(self) -> Dict[str, Any]:
        """
        Non-secret metadata resolved during CTP / client construction that the
        caller (Data Collector) may need to include in emitted records — e.g.,
        the API base URL resolved from a login response, used by callers to
        construct stable customer-facing links to integration UIs.

        Default: empty dict. Override on subclasses that have something
        useful to expose. Values must be JSON-serializable.
        """
        return {}

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
