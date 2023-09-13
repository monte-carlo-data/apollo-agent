import logging
from typing import Any, List, Dict, Union, Optional, Callable

logger = logging.getLogger(__name__)


class AgentError(Exception):
    pass


class AgentEvaluationUtils:
    @classmethod
    def execute(cls, context: Dict, commands: List[Union[Dict, List]]) -> Optional[Any]:
        try:
            last_result: Optional[Any] = None
            for command in commands:
                if isinstance(command, list):
                    last_result = cls._execute_chained_commands(command, context)
                else:
                    last_result = cls._execute_single_command(command, context)
            return last_result
        except Exception as ex:
            logger.exception(
                "Exception occurred executing commands",
                extra={
                    "commands": commands,
                },
            )
            return {"__error__": str(ex)}

    @classmethod
    def _execute_single_command(
        cls, command: Dict, context: Dict, target: Optional[Any] = None
    ) -> Optional[Any]:
        if not target:
            target_name = command.get("target") or "_client"
            if target_name not in context:
                raise AgentError(f"{target} not found in context")
            target = context[target_name]
        method = cls._resolve_method(target, command["method"])
        result = method(*command.get("args", []), **command.get("kwargs", {}))
        if store := command.get("store"):
            context[store] = result
        return result

    @staticmethod
    def _resolve_method(target: Any, method_name: str) -> Callable:
        if hasattr(target, method_name):
            return getattr(target, method_name)
        if hasattr(target, "wrapped_client"):
            client = getattr(target, "wrapped_client")
            if hasattr(client, method_name):
                return getattr(client, method_name)
        raise AttributeError(f"Failed to resolve method {method_name}")

    @classmethod
    def _execute_chained_commands(
        cls, commands: List[Dict], context: Dict
    ) -> Optional[Any]:
        target: Optional[Any] = None
        for command in commands:
            target = cls._execute_single_command(command, context, target)
        return target


class Agent:
    def execute(self, client: Any, commands: List[Union[Dict, List]]) -> Optional[Any]:
        context = {
            "_client": client,
        }
        return AgentEvaluationUtils.execute(context, commands)
