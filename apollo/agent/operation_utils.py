import logging
from typing import Dict, List, Iterable, Any

from apollo.agent.evaluation_utils import AgentEvaluationUtils
from apollo.agent.models import AgentCommand

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

    def build_list(self, var_name: str, items: Iterable, item_call: Dict) -> List:
        return [self._single_call(item_call, var_name, value) for value in items]

    def _single_call(self, call: Dict, var_name: str, value: Any) -> Any:
        command = AgentCommand.from_dict(call)
        return self._execute_with_temp_var(var_name, value, command)

    def _execute_with_temp_var(
        self, var_name: str, value: Any, command: AgentCommand
    ) -> Any:
        old_value = self._context.get(var_name)
        self._context[var_name] = value
        result = AgentEvaluationUtils.execute_command(command, self._context)
        if old_value:
            self._context[var_name] = old_value
        else:
            self._context.pop(var_name)
        return result
