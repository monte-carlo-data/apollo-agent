import logging
from typing import Any, Callable, Optional, Dict, List, cast

from apollo.agent.annotate_logger import annotate_logger
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.models import (
    AgentError,
    AgentCommand,
    AgentScript,
)
from apollo.agent.constants import (
    ATTRIBUTE_NAME_REFERENCE,
    ATTRIBUTE_NAME_TYPE,
    ATTRIBUTE_VALUE_TYPE_CALL,
    CONTEXT_VAR_CLIENT,
)
from apollo.agent.scripts import AgentScriptContext, execute_script
from apollo.agent.serde import decode_dict_value
from apollo.agent.utils import AgentUtils
from apollo.integrations.base_proxy_client import BaseProxyClient

logger = logging.getLogger(__name__)


class AgentEvaluationUtils:
    """
    Utility class that performs operation commands, it supports "chains" created using the "next"
    attribute in a command (which means to use the result of a given command as the target for
    the "next" call).
    """

    @classmethod
    def execute(
        cls,
        context: Dict,
        logging_utils: LoggingUtils,
        operation_name: str,
        commands: List[AgentCommand],
        trace_id: str,
    ) -> Optional[Any]:
        """
        Executes a list of commands from an operation, returns the result of the
        last command in the list.
        :param context: the context containing variables to use as targets.
        :param logging_utils: helper class to create the log payload.
        :param operation_name: name of the operation being executed, for logging purposes only.
        :param commands: the list of commands to execute.
        :param trace_id: trace id of the operation being executed, for logging purposes only.
        :return: the result of the last command in the list.
        """
        client: BaseProxyClient = cast(BaseProxyClient, context.get(CONTEXT_VAR_CLIENT))
        try:
            last_result: Optional[Any] = None
            for command in commands:
                last_result = cls._execute_command(command, context)
            return client.process_result(last_result)
        except Exception as ex:
            should_log = client.should_log_exception(ex)
            log_method = logger.exception if should_log else logger.info
            message = "Exception occurred executing operation"
            if not should_log:
                message += f": {ex}"
            log_method(
                message,
                extra=logging_utils.build_extra(
                    trace_id=trace_id,
                    operation_name=operation_name,
                ),
            )
            return AgentUtils.response_for_last_exception(client=client)

    @classmethod
    def execute_script(
        cls,
        context: Dict,
        logging_utils: LoggingUtils,
        operation_name: str,
        script: AgentScript,
        trace_id: str,
    ) -> Optional[Any]:
        """
        Executes a script by getting a proxy client from the context.
        :param context: the context containing variables to use as targets.
        :param logging_utils: helper class to create the log payload.
        :param operation_name: name of the operation being executed, for logging purposes only.
        :param script: the script to execute
        :param trace_id: trace id of the operation being executed, for logging purposes only.
        :return: the result of the script execution.
        """
        client: BaseProxyClient = cast(BaseProxyClient, context.get(CONTEXT_VAR_CLIENT))
        try:
            client = cls._resolve_context_variable(context, CONTEXT_VAR_CLIENT)
            script_context = AgentScriptContext(
                logger=annotate_logger(
                    logger,
                    logging_utils.build_extra(
                        trace_id=trace_id,
                        operation_name=operation_name,
                    ),
                )
            )
            last_result = execute_script(script, client, script_context)
            return client.process_result(last_result)
        except Exception as ex:
            should_log = client.should_log_exception(ex)
            log_method = logger.exception if should_log else logger.info
            message = "Exception occurred executing operation"
            if not should_log:
                message += f": {ex}"
            log_method(
                message,
                extra=logging_utils.build_extra(
                    trace_id=trace_id,
                    operation_name=operation_name,
                ),
            )
            return AgentUtils.response_for_last_exception(client=client)

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
            try:
                result = method(
                    *cls._resolve_args(command.args, context),
                    **cls._resolve_kwargs(command.kwargs, context),
                )
            except Exception as ex:
                logger.info(f"Error calling method {command.method}: {ex}")
                raise
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
        """
        Utility method used to "resolve" a list of arguments, processing method calls and references to variables.
        When a call is found as an argument, the call is performed and the result value used as the argument value.
        When a variable reference is found, the value is obtained from the context and used as the argument value.
        :param args: A list of arguments
        :param context: A context dictionary holding variable values
        :return: The new list of arguments
        """
        if not args:
            return []
        return [cls.resolve_arg_value(arg, context) for arg in args]

    @classmethod
    def _resolve_kwargs(cls, kwargs: Optional[Dict], context: Dict) -> Dict:
        """
        Similar to `resolve_args` but processing keyword arguments.
        :param kwargs: keyword arguments to process
        :param context: context holding variables to use for replacement
        :return: The new keyword arguments
        """
        if not kwargs:
            return {}
        return {
            key: cls.resolve_arg_value(value, context) for key, value in kwargs.items()
        }

    @classmethod
    def resolve_arg_value(cls, value: Any, context: Dict) -> Any:
        """
        Resolves a single argument value, it checks for a variable reference or a method call.
        A variable reference is identified by a dictionary containing "__reference__", the value for this property is
        the name of the variable to look for. This method will return the value of the variable in "context" or fail
        if that variable is not present.
        A call is identified by a dictionary containing a "__type__" attribute with value "call", the rest of the
        attributes are expected to define an "AgentCommand" object defining the call to perform. This method will
        perform the call and return its result.
        If value is not a variable reference or a method call, it is returned as the result for this method.
        :param value: the value present in args or kwargs
        :param context: the dictionary holding values for variables
        :return: The value for the referenced variable, the result of performing the specified call or just the input
            value.
        """
        if isinstance(value, Dict):
            if ATTRIBUTE_NAME_REFERENCE in value:
                return cls._resolve_context_variable(
                    context, value[ATTRIBUTE_NAME_REFERENCE]
                )
            elif value.get(ATTRIBUTE_NAME_TYPE) == ATTRIBUTE_VALUE_TYPE_CALL:
                return cls._execute_single_command(
                    AgentCommand.from_dict(value), context
                )
            elif ATTRIBUTE_NAME_TYPE in value:
                return decode_dict_value(value)
        return value

    @staticmethod
    def _resolve_context_variable(context: Dict, var_name: str) -> Any:
        """
        Resolves the value of the variable in context, raises an AgentError if the variable is not present in context.
        :param context: the dictionary containing variables
        :param var_name: the name of the variable to return
        :return: the value for the specified variable in context, raises an AgentError if not present.
        """
        if var_name not in context:
            raise AgentError(f"{var_name} not found in context")
        return context[var_name]
