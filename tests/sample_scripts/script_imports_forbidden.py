# disallowing import of most modules, only allowing those listed on
# constants.AGENT_SCRIPT_BUILTIN_MODULES
import os
from typing import Any


def execute_script_handler(client: Any, context: Any, sql_query: str):
    return os.getcwd()
