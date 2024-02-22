import logging
import types
from asyncio import Protocol
from typing import Any, Optional, cast

from RestrictedPython import compile_restricted, safe_builtins

from apollo.agent.constants import AGENT_SCRIPT_ENTRYPOINT
from apollo.agent.models import AgentScript
from apollo.integrations.base_proxy_client import BaseProxyClient


class AgentScriptContext:
    """Functions available to the script through its script context"""

    def __init__(self, logger: logging.Logger):
        self._logger = logger

    @property
    def logger(self):
        return self._logger


class AgentScriptEntrypoint(Protocol):
    """Protocol for the script entrypoint function"""

    def __call__(
        self, client: Any, script_context: AgentScriptContext, **kwargs: Any
    ) -> Any:
        ...


def execute_script(
    script: AgentScript, client: BaseProxyClient, script_context: AgentScriptContext
) -> Optional[Any]:
    """
    Executes a script with the given client and context
    :param script: the script definition to execute
    :param client: the connection client
    :param script_context: a set of helpers and context information for the script function
    :return: the result of the script entrypoint function
    """

    module_bytecode = {}
    for module in script.modules:
        byte_code = compile_restricted(module.source, module.name, "exec")
        module_bytecode[module.name] = byte_code

    cached_modules = {}

    def import_script_module(name: str, *args: Any, **kwargs: Any) -> types.ModuleType:
        if name not in module_bytecode:
            raise ImportError(f"Module '{name}' not found in script")
        if name not in cached_modules:
            imported_module = types.ModuleType(name)
            imported_module.__dict__.update(script_globals)
            # at module level, globals and locals are the same dictionary
            exec(module_bytecode[name], imported_module.__dict__)
            cached_modules[name] = imported_module

        cached_module = cached_modules[name]
        return cached_module

    script_globals = {
        "__builtins__": {**safe_builtins, "__import__": import_script_module}
    }

    entry_module = module_bytecode[script.entry_module]
    loc = {**script_globals}
    # at module level, globals and locals are the same dictionary
    exec(entry_module, loc)
    if AGENT_SCRIPT_ENTRYPOINT not in loc:
        raise ValueError(
            f"'{AGENT_SCRIPT_ENTRYPOINT}' function not found in agent script"
        )
    entry_point = cast(AgentScriptEntrypoint, loc[AGENT_SCRIPT_ENTRYPOINT])
    return entry_point(client.wrapped_client, script_context, **script.kwargs)
