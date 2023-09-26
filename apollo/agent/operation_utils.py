import logging
from typing import Dict

from apollo.agent.evaluation_utils import AgentEvaluationUtils

logger = logging.getLogger(__name__)


class OperationUtils:
    def __init__(self, context: Dict):
        self._context = context

    def build_dict(self, **kwargs) -> Dict:
        return {
            key: AgentEvaluationUtils.resolve_arg_value(value, self._context)
            for key, value in kwargs.items()
        }
