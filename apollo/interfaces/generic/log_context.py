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
            return record

        extra: Dict = getattr(record, self._attr_name, {})
        extra.update(self._context)
        setattr(record, self._attr_name, extra)

        return record
