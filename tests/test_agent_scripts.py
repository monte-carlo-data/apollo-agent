from os import path
from unittest import TestCase

from apollo.agent.agent import Agent
from apollo.common.agent.constants import ATTRIBUTE_NAME_ERROR
from apollo.agent.logging_utils import LoggingUtils
from apollo.common.agent.models import AgentScript
from tests.sample_proxy_client import SampleProxyClient


def read_script_source(script_name: str):
    script_path = f"{path.dirname(__file__)}/sample_scripts/{script_name}.py"
    with open(script_path, "r") as f:
        return f.read()


class AgentScriptsTests(TestCase):
    def setUp(self) -> None:
        self._query = "SELECT * FROM table"
        self._client = SampleProxyClient()

    def test_fetch_rows(self):
        result = Agent(LoggingUtils())._execute_script(
            self._client,
            "test",
            AgentScript.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "entry_module": "main",
                    "modules": [
                        {
                            "name": "main",
                            "source": read_script_source("script_fetch_rows"),
                        }
                    ],
                    "kwargs": {
                        "sql_query": self._query,
                    },
                }
            ),
        )
        cursor = SampleProxyClient().wrapped_client.cursor()
        cursor.execute(self._query)
        expected_result = {"rows": cursor.fetchmany()}
        self.assertEqual(expected_result, result)

    def test_builtins_valid(self):
        # provide a path name to allow debugging & better error stack
        entry_module = (
            f"{path.dirname(__file__)}/sample_scripts/script_builtins_valid.py"
        )
        result = Agent(LoggingUtils())._execute_script(
            self._client,
            "test",
            AgentScript.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "entry_module": entry_module,
                    "modules": [
                        {
                            "name": entry_module,
                            "source": read_script_source("script_builtins_valid"),
                        }
                    ],
                    "kwargs": {
                        "sql_query": self._query,
                    },
                }
            ),
        )
        self.assertEqual(result, "all is good")

    def test_imports_forbidden(self):
        result = Agent(LoggingUtils())._execute_script(
            self._client,
            "test",
            AgentScript.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "entry_module": "main",
                    "modules": [
                        {
                            "name": "main",
                            "source": read_script_source("script_imports_forbidden"),
                        }
                    ],
                    "kwargs": {},
                }
            ),
        )
        self.assertEqual(
            result[ATTRIBUTE_NAME_ERROR],
            "Module 'os' not found in script nor in built-in modules",
        )

    def test_imports_valid(self):
        result = Agent(LoggingUtils())._execute_script(
            self._client,
            "test",
            AgentScript.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "entry_module": "main",
                    "modules": [
                        {
                            "name": "helpers_foo",
                            "source": read_script_source("helpers_foo"),
                        },
                        {
                            "name": "helpers_bar",
                            "source": read_script_source("helpers_bar"),
                        },
                        {
                            "name": "main",
                            "source": read_script_source("script_imports_valid"),
                        },
                    ],
                    "kwargs": {
                        "sql_query": self._query,
                    },
                }
            ),
        )
        self.assertEqual(result, "bar_foobar_foobar")

    def test_builtins_forbidden(self):
        result = Agent(LoggingUtils())._execute_script(
            self._client,
            "test",
            AgentScript.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "entry_module": "main",
                    "modules": [
                        {
                            "name": "main",
                            "source": read_script_source("script_builtins_forbidden"),
                        }
                    ],
                    "kwargs": {},
                }
            ),
        )
        self.assertEqual(
            result[ATTRIBUTE_NAME_ERROR], "('Line 1: Exec calls are not allowed.',)"
        )

    def test_no_execute_script_handler(self):
        result = Agent(LoggingUtils())._execute_script(
            self._client,
            "test",
            AgentScript.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "entry_module": "main",
                    "modules": [
                        {
                            "name": "main",
                            "source": read_script_source(
                                "script_no_execute_script_handler"
                            ),
                        }
                    ],
                    "kwargs": {
                        "sql_query": self._query,
                    },
                }
            ),
        )
        self.assertEqual(
            result[ATTRIBUTE_NAME_ERROR],
            "'execute_script_handler' function not found in agent script",
        )

    def test_underscore_forbidden(self):
        result = Agent(LoggingUtils())._execute_script(
            self._client,
            "test",
            AgentScript.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "entry_module": "main",
                    "modules": [
                        {
                            "name": "main",
                            "source": read_script_source("script_underscore_forbidden"),
                        }
                    ],
                    "kwargs": {
                        "sql_query": self._query,
                    },
                }
            ),
        )
        self.assertEqual(
            result[ATTRIBUTE_NAME_ERROR],
            '(\'Line 3: "_a" is an invalid variable name because it starts with "_"\',)',
        )

    def test_inplace_var_forbidden(self):
        result = Agent(LoggingUtils())._execute_script(
            self._client,
            "test",
            AgentScript.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "entry_module": "main",
                    "modules": [
                        {
                            "name": "main",
                            "source": read_script_source(
                                "script_inplace_var_forbidden"
                            ),
                        }
                    ],
                    "kwargs": {
                        "sql_query": self._query,
                    },
                }
            ),
        )
        self.assertEqual(
            result[ATTRIBUTE_NAME_ERROR],
            "name '_inplacevar_' is not defined",
        )

    def test_ann_assignment_forbidden(self):
        result = Agent(LoggingUtils())._execute_script(
            self._client,
            "test",
            AgentScript.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "entry_module": "main",
                    "modules": [
                        {
                            "name": "main",
                            "source": read_script_source(
                                "script_ann_assignment_forbidden"
                            ),
                        }
                    ],
                    "kwargs": {
                        "sql_query": self._query,
                    },
                }
            ),
        )
        self.assertEqual(
            result[ATTRIBUTE_NAME_ERROR],
            "('Line 4: AnnAssign statements are not allowed.',)",
        )
