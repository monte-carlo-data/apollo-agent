import logging
import types
from asyncio import Protocol
from typing import Any, Optional, cast

from RestrictedPython import compile_restricted, safe_builtins, utility_builtins
from RestrictedPython.Eval import default_guarded_getitem, default_guarded_getiter
from RestrictedPython.Guards import (
    guarded_iter_unpack_sequence,
    guarded_unpack_sequence,
    safer_getattr,
)

from apollo.agent.constants import AGENT_SCRIPT_ENTRYPOINT, AGENT_SCRIPT_BUILTIN_MODULES
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
    ) -> Any: ...


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
        """Custom implementation of __import__ for agent scripts. Restrict imports
        to a pre-defined list of modules or modules provided on the agent script."""
        if name in AGENT_SCRIPT_BUILTIN_MODULES:
            return __import__(name, *args, **kwargs)
        if name not in module_bytecode:
            raise ImportError(
                f"Module '{name}' not found in script nor in built-in modules"
            )
        if name not in cached_modules:
            imported_module = types.ModuleType(name)
            imported_module.__dict__.update(script_globals)
            # at module level, globals and locals are the same dictionary
            exec(module_bytecode[name], imported_module.__dict__)
            cached_modules[name] = imported_module

        cached_module = cached_modules[name]
        return cached_module

    # we don't use the default limited builtins provided by RestrictedPython as we don't
    # intend to limit these
    type_builtins = {
        "dict": dict,
        "list": list,
        "iter": iter,
    }

    # support for classes and special constructs
    class_manipulation = {
        "staticmethod": staticmethod,
        "classmethod": classmethod,
        "property": property,
        "__name__": "__main__",
        "__metaclass__": type,
        "_getattr_": getattr,
        "_getitem_": default_guarded_getitem,
        "_getiter_": default_guarded_getiter,
        "_iter_unpack_sequence_": guarded_iter_unpack_sequence,
        "_unpack_sequence_": guarded_unpack_sequence,
        "_write_": lambda o: o,
    }

    # additional helpers
    helper_builtins = {
        "enumerate": enumerate,
        "filter": filter,
        "reversed": reversed,
        "next": next,
        "hasattr": hasattr,
        "getattr": safer_getattr,
        "map": map,
        "max": max,
        "min": min,
        "sum": sum,
        "all": all,
        "any": any,
        "dir": dir,
    }

    script_globals = {
        "__builtins__": {
            **safe_builtins,
            **utility_builtins,
            **type_builtins,
            **class_manipulation,
            **helper_builtins,
            "__import__": import_script_module,
        }
    }

    entry_module = module_bytecode[script.entry_module]

    # at module level, globals and locals are the same dictionary
    loc = {**script_globals}
    exec(entry_module, loc)
    if AGENT_SCRIPT_ENTRYPOINT not in loc:
        raise ValueError(
            f"'{AGENT_SCRIPT_ENTRYPOINT}' function not found in agent script"
        )
    entry_point = cast(AgentScriptEntrypoint, loc[AGENT_SCRIPT_ENTRYPOINT])
    return entry_point(client.wrapped_client, script_context, **script.kwargs)
