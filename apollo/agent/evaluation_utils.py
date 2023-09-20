import logging
from typing import Any, Callable, Optional, Dict, List, Iterable

from apollo.agent.models import (
    AgentError,
    AgentCommand,
    ATTRIBUTE_NAME_REFERENCE,
    ATTRIBUTE_NAME_TYPE,
    ATTRIBUTE_VALUE_TYPE_CALL,
    CONTEXT_VAR_CLIENT,
)
from apollo.agent.utils import AgentUtils

logger = logging.getLogger(__name__)


class AgentEvaluationUtils:
    """
    Utility class that performs operation commands, it supports "chains" created using the "next"
    attribute in a command and it means to use the result of a given command as the target for
    the "next" call.
    """

    @classmethod
    def execute(cls, context: Dict, commands: List[AgentCommand]) -> Optional[Any]:
        """
        Executes a list of commands from an operation, returns the result of the
        last command in the list.
        :param context: the context containing variables to use as targets.
        :param commands: the list of commands to execute.
        :return: the result of the last command in the list.
        """
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
            return AgentUtils.response_for_last_exception()

    @classmethod
    def _execute_command(cls, command: AgentCommand, context: Dict) -> Optional[Any]:
        """
        Execute a single command, if the command is the root of a chain (using next attribute)
        the whole chain is executed.
        :param command: the command to execute
        :param context: the context including variables to use as targets
        :return: the result of the command (or last command in the chain)
        """
        to_execute_command: Optional[AgentCommand] = command
        result: Optional[Any] = None
        target: Optional[Any] = None
        while to_execute_command:
            result = cls._execute_single_command(to_execute_command, context, target)
            target = result
            to_execute_command = to_execute_command.next
        return result

    @classmethod
    def _execute_single_command(
        cls, command: AgentCommand, context: Dict, target: Optional[Any] = None
    ) -> Optional[Any]:
        """
        Executes a single command and returns the result
        :param command: the command to execute
        :param context: the context including variables to use as targets
        :param target: the optional target of the call, if present overrides the target defined in the command
        :return: the result of the command
        """
        if not target:
            target_name = command.target or CONTEXT_VAR_CLIENT
            target = cls._resolve_context_variable(context, target_name)
        method = cls._resolve_method(target, command.method)
        if isinstance(method, Callable):
            result = method(
                *cls._resolve_args(command.args, context),
                **cls._resolve_kwargs(command.kwargs, context),
            )
        else:
            result = method  # assume it is a property
        if store := command.store:
            context[store] = result
        return result

    @staticmethod
    def _resolve_method(target: Any, method_name: str) -> Callable:
        """
        Methods are searched first in the proxy client object, allowing to call "extension" methods.
        If not present then it's searched in the wrapped_client object (the driver client).
        :param target: the target object, usually the proxy client
        :param method_name: the method to search for
        :return: the method found, AttributeError is raised if no method is found.
        """
        if hasattr(target, method_name):
            return getattr(target, method_name)
        if hasattr(target, "wrapped_client"):
            client = getattr(target, "wrapped_client")
            if hasattr(client, method_name):
                return getattr(client, method_name)
        raise AttributeError(f"Failed to resolve method {method_name}")

    @classmethod
    def _resolve_args(cls, args: Optional[List[Any]], context: Dict) -> List[Any]:
        if not args:
            return []
        return [cls.resolve_arg_value(arg, context) for arg in args]

    @classmethod
    def _resolve_kwargs(cls, kwargs: Optional[Dict], context: Dict) -> Dict:
        if not kwargs:
            return {}
        return {
            key: cls.resolve_arg_value(value, context) for key, value in kwargs.items()
        }

    @classmethod
    def resolve_arg_value(cls, value: Any, context: Dict) -> Any:
        if isinstance(value, Dict):
            if ATTRIBUTE_NAME_REFERENCE in value:
                return cls._resolve_context_variable(
                    context, value[ATTRIBUTE_NAME_REFERENCE]
                )
            elif value.get(ATTRIBUTE_NAME_TYPE) == ATTRIBUTE_VALUE_TYPE_CALL:
                return cls._execute_single_command(
                    AgentCommand.from_dict(value), context
                )
        return value

    @staticmethod
    def _resolve_context_variable(context: Dict, var_name: str) -> Any:
        if var_name not in context:
            raise AgentError(f"{var_name} not found in context")
        return context[var_name]
