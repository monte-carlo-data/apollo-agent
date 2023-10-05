import logging
from typing import Dict

from apollo.agent.evaluation_utils import AgentEvaluationUtils

logger = logging.getLogger(__name__)


class OperationUtils:
    def __init__(self, context: Dict):
        self._context = context

    def build_dict(self, **kwargs) -> Dict:
        """
        Utility method used by clients to create a dictionary, usually with the result of multiple calls.
        For example, database cursor uses this to return results along description (schema information) and row count.
        :param kwargs: keyword arguments to use to build the dictionary, each argument is "processed" resolving
            references to calls or variables.
        :return: A new dictionary based on the keyword arguments received.
        """
        return {
            key: AgentEvaluationUtils.resolve_arg_value(value, self._context)
            for key, value in kwargs.items()
        }
