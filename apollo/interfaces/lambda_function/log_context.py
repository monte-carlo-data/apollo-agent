from copy import deepcopy
from typing import Dict, Any

from apollo.agent.log_context import AgentLogContext


class LambdaLogContext(AgentLogContext):
    """
    Implements AgentLogContext, stores the context information and uses it in the filter method by appending it
    to the `extra` information in the log record.
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

        extra: Dict = getattr(record, "extra", {})
        extra.update(self._context)
        record.extra = extra
        return record
