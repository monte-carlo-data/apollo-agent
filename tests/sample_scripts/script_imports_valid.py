from typing import Any

import helpers_foo
from helpers_foo import foobar as foobar2


def bar():
    return "bar"


def execute_script_handler(client: Any, context: Any, sql_query: str):
    return f"{bar()}_{helpers_foo.foobar()}_{foobar2()}"
