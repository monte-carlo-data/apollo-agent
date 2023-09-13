import logging
from typing import Any, List, Dict, Union, Optional, Callable

from apollo.agent.models import AgentOperation, AgentCommand

logger = logging.getLogger(__name__)


class AgentError(Exception):
    pass


class AgentEvaluationUtils:
    @classmethod
    def execute(cls, context: Dict, commands: List[AgentCommand]) -> Optional[Any]:
        try:
            last_result: Optional[Any] = None
            for command in commands:
                last_result = cls._execute_command(command, context)
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
    def _execute_command(
        cls, command: AgentCommand, context: Dict, target: Optional[Any] = None
    ) -> Optional[Any]:
        to_execute_command: Optional[AgentCommand] = command
        result: Optional[Any] = None
        while to_execute_command:
            result = cls._execute_single_command(to_execute_command, context, target)
            target = result
            to_execute_command = to_execute_command.next
        return result

    @classmethod
    def _execute_single_command(
        cls, command: AgentCommand, context: Dict, target: Optional[Any] = None
    ) -> Optional[Any]:
        if not target:
            target_name = command.target or "_client"
            if target_name not in context:
                raise AgentError(f"{target} not found in context")
            target = context[target_name]
        method = cls._resolve_method(target, command.method)
        args = command.args or []
        kwargs = command.kwargs or {}
        result = method(*args, **kwargs)
        if store := command.store:
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


class Agent:
    def execute(self, client: Any, operation: AgentOperation) -> Optional[Any]:
        context = {
            "_client": client,
        }
        return AgentEvaluationUtils.execute(context, operation.commands)
