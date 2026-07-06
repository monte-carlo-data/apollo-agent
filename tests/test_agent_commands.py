import logging
from unittest import TestCase
from unittest.mock import create_autospec, call

from apollo.agent.agent import Agent
from apollo.agent.evaluation_utils import AgentEvaluationUtils
from apollo.agent.log_context import AgentLogContext
from apollo.agent.logging_utils import LoggingUtils
from apollo.common.agent.constants import CONTEXT_VAR_CLIENT
from apollo.common.agent.models import AgentCommand, AgentCommands
from tests.sample_proxy_client import SampleProxyClient

_EVALUATION_LOGGER = "apollo.agent.evaluation_utils"


class AgentCommandsTests(TestCase):
    def setUp(self) -> None:
        self._query = "SELECT * FROM table"
        self._expected_result = SampleProxyClient().execute_and_fetch(self._query)

        self._client = SampleProxyClient()

    def test_single_call_wrapper_method(self):
        result = Agent(LoggingUtils())._execute(
            self._client,
            "test",
            AgentCommands.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "commands": [
                        {
                            "method": "execute_and_fetch",
                            "args": [self._query],
                        }
                    ],
                }
            ),
        )
        self.assertEqual(self._expected_result, result)

    def test_commands_list_client_methods(self):
        # _client.execute_query(query)
        # _client.fetch_results()

        result = Agent(LoggingUtils())._execute(
            self._client,
            "test",
            AgentCommands.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "commands": [
                        {
                            "method": "execute_query",
                            "kwargs": {
                                "query": self._query,
                            },
                        },
                        {
                            "method": "fetch_results",
                        },
                    ],
                }
            ),
        )
        self.assertEqual(self._expected_result, result)

    def test_store_and_commands_list_cursor_methods(self):
        # _cursor = _client.cursor()
        # _cursor.cursor_execute_query(query)
        # _cursor.cursor_fetch_results()
        result = Agent(LoggingUtils())._execute(
            self._client,
            "test",
            AgentCommands.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "commands": [
                        {
                            "method": "cursor",
                            "store": "_cursor",
                        },
                        {
                            "target": "_cursor",
                            "method": "cursor_execute_query",
                            "kwargs": {
                                "query": self._query,
                            },
                        },
                        {
                            "target": "_cursor",
                            "method": "cursor_fetch_results",
                        },
                    ],
                }
            ),
        )
        self.assertEqual(self._expected_result, result)

    def test_store_and_chained_cursor_methods(self):
        # _cursor = _client.cursor()
        # _cursor.cursor_execute_query(query).cursor_fetch_results()
        result = Agent(LoggingUtils())._execute(
            self._client,
            "test",
            AgentCommands.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "commands": [
                        {
                            "method": "cursor",
                            "store": "_cursor",
                        },
                        {
                            "target": "_cursor",
                            "method": "cursor_execute_query",
                            "kwargs": {
                                "query": self._query,
                            },
                            "next": {
                                "method": "query_results",
                            },
                        },
                    ],
                }
            ),
        )
        self.assertEqual(self._expected_result, result)

    def test_log_context(self):
        agent = Agent(LoggingUtils())
        log_context = create_autospec(AgentLogContext)
        agent.log_context = log_context

        trace_id = "135"
        agent.health_information(trace_id)

        log_context.set_agent_context.assert_has_calls(
            [
                call(
                    {
                        "mcd_operation_name": "health_information",
                        "mcd_trace_id": trace_id,
                    }
                ),
                call({}),
            ]
        )


class _SecretRaisingClient(SampleProxyClient):
    _SECRET = "presigned-url-token-super-secret"

    def download_bytes(self, *args, **kwargs):
        raise ValueError(f"oversized payload {self._SECRET}")


class AgentCommandFailureLoggingTests(TestCase):
    def test_method_resolution_failure_logs_error_with_stable_message(self):
        with self.assertLogs(_EVALUATION_LOGGER, level=logging.ERROR) as captured:
            with self.assertRaises(AttributeError):
                AgentEvaluationUtils._resolve_method(
                    SampleProxyClient(), "does_not_exist"
                )

        self.assertEqual(1, len(captured.records))
        record = captured.records[0]
        self.assertEqual(logging.ERROR, record.levelno)
        self.assertIn("Failed to resolve method", record.getMessage())
        self.assertIn("does_not_exist", record.getMessage())

    def test_method_invocation_failure_logs_error_without_leaking_exception(self):
        client = _SecretRaisingClient()
        context = {CONTEXT_VAR_CLIENT: client}
        command = AgentCommand.from_dict({"method": "download_bytes"})

        with self.assertLogs(_EVALUATION_LOGGER, level=logging.ERROR) as captured:
            with self.assertRaises(ValueError):
                AgentEvaluationUtils._execute_single_command(command, context)

        self.assertEqual(1, len(captured.records))
        record = captured.records[0]
        self.assertEqual(logging.ERROR, record.levelno)
        self.assertIsNotNone(record.exc_info)
        message = record.getMessage()
        self.assertIn("download_bytes", message)
        self.assertIn("ValueError", message)
        self.assertNotIn(_SecretRaisingClient._SECRET, message)
