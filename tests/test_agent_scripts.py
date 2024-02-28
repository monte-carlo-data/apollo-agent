from unittest import TestCase
from unittest.mock import create_autospec, call

from apollo.agent.agent import Agent
from apollo.agent.constants import ATTRIBUTE_NAME_ERROR
from apollo.agent.log_context import AgentLogContext
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.models import AgentCommands, AgentScript
from tests.sample_proxy_client import SampleProxyClient


class AgentScriptsTests(TestCase):
    def setUp(self) -> None:
        self._query = "SELECT * FROM table"
        self._client = SampleProxyClient()

    def test_call_client_method_and_return(self):
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
                            "source": """
def execute_script_handler(client, context, sql_query):        
    if client is None:
        raise Exception('is none')    
    with client.cursor() as cursor:        
        cursor.execute(sql_query)
        return {'rows': cursor.fetchmany(10)}
""",
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

    def test_use_import_fails_for_unexisting_module(self):
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
                            "source": """
import os
def execute_script_handler(client, context, sql_query):                
    return os.getcwd()
    """,
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

    def test_use_import_fails_for_existing_module(self):
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
                            "source": """
import helpers_bar                
def foobar():                
    return 'foo' + helpers_bar.bar()
""",
                        },
                        {
                            "name": "helpers_bar",
                            "source": """                
def bar():                
    return "bar"
""",
                        },
                        {
                            "name": "main",
                            "source": """
import helpers_foo
from helpers_foo import foobar as foobar2   
def bar():
    return "bar"
def execute_script_handler(client, context, sql_query):                    
    return f'{bar()}_{helpers_foo.foobar()}_{foobar2()}'
        """,
                        },
                    ],
                    "kwargs": {
                        "sql_query": self._query,
                    },
                }
            ),
        )
        self.assertEqual(result, "bar_foobar_foobar")

    def test_use_exec_fails(self):
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
                            "source": """
exec("return 1")
            """,
                        }
                    ],
                    "kwargs": {},
                }
            ),
        )
        self.assertEqual(
            result[ATTRIBUTE_NAME_ERROR], "('Line 2: Exec calls are not allowed.',)"
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
                            "source": """
def some_func(client, context, sql_query):        
    if client is None:
        raise Exception('is none')    
    with client.cursor() as cursor:        
        cursor.execute(sql_query)
        return {'rows': cursor.fetchmany(10)}
""",
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
