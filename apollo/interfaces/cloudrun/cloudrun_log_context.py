from copy import deepcopy
from typing import Dict, Any

from apollo.agent.log_context import AgentLogContext


class CloudRunLogContext(AgentLogContext):
    """
    Implements AgentLogContext, stores the context information and uses it in the filter method by appending it
    to the `json_fields` information in the log record.
    """

    def __init__(self):
        self._context: Dict = {}

    def set_agent_context(self, context: Dict):
        self._context = deepcopy(context)

    def filter(self, record: Any) -> Any:
        """
        Updates the log record with the agent context
        """
        if not self._context:
            return record

        json_fields: Dict = getattr(record, "json_fields", {})
        json_fields.update(self._context)
        record.json_fields = json_fields
        return record
