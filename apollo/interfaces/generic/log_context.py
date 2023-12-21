import logging
from copy import deepcopy
from typing import Dict, Any, Optional

from apollo.agent.log_context import AgentLogContext


class BaseLogContext(AgentLogContext):
    """
    Implements AgentLogContext, stores the context information and uses it in the filter method by appending it
    to the `extra` information in the log record.
    The name of the "extra" attribute can be configured to something different, for example GCP requires that
    attribute to be "json_fields".
    """

    def __init__(self, attr_name: str = "extra"):
        self._context: Dict = {}
        self._backup_context: Optional[Dict] = None
        self._attr_name = attr_name

    def install(self):
        root_logger = logging.getLogger()
        for h in root_logger.handlers:
            h.addFilter(lambda record: self._filter(record))

    def set_agent_context(self, context: Dict):
        self._context = deepcopy(context)

    def _filter(self, record: Any) -> Any:
        """
        Updates the log record with the agent context
        """
        if not self._context:
            if self._backup_context:
                logging.getLogger().warning(
                    f"NO CONTEXT, RECURSIVE CALL: {self._backup_context}"
                )
            return record

        self._backup_context = self._context
        self._context = {}
        try:
            # don't update the attribute if already present, causing a recursion issue in Azure
            if hasattr(record, self._attr_name):
                extra: Dict = getattr(record, self._attr_name, {})
                extra.update(self._backup_context)
            else:
                setattr(record, self._attr_name, self._backup_context)
        finally:
            self._context = self._backup_context
            self._backup_context = None

        return record
