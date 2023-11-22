import json
from unittest import TestCase
from unittest.mock import (
    MagicMock,
    Mock,
    create_autospec,
    patch,
)

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.models import (
    AgentCommand,
    AgentOperation,
)
from apollo.integrations.storage.storage_proxy_client import StorageProxyClient
from sample_proxy_client import SampleProxyClient


class AgentResponseTests(TestCase):
    def setUp(self):
        self._agent = Agent(LoggingUtils())
        self._client = SampleProxyClient()
        self._trace_id = "test_trace_id"
        self._commands = [AgentCommand(method="foo")]

    @patch.object(Agent, "_execute")
    def test_no_pre_signed_urls(self, mock_execute: MagicMock):
        mock_execute.return_value = {"foo": "bar"}
        response = self._agent._execute_client_operation(
            connection_type="test",
            client=self._client,
            operation_name="test",
            operation=AgentOperation(
                trace_id=self._trace_id,
                commands=self._commands,
                response_size_limit_bytes=0,
            ),
        )
        self.assertEqual(
            {"__mcd_result__": {"foo": "bar"}, "__mcd_trace_id__": self._trace_id},
            response.result,
        )

    @patch.object(Agent, "_execute")
    @patch("apollo.agent.agent.StorageProxyClient")
    def test_use_pre_signed_urls(self, storage_mock: Mock, mock_execute: Mock):
        mock_storage_client = create_autospec(StorageProxyClient)
        storage_mock.return_value = mock_storage_client
        mock_storage_client.write.return_value = None
        mock_storage_client.generate_presigned_url.return_value = (
            "https://example.com/fizz_buzz"
        )
        mock_execute.return_value = {"fizz": "buzz"}
        response = self._agent._execute_client_operation(
            connection_type="test",
            client=self._client,
            operation_name="test",
            operation=AgentOperation(
                trace_id=self._trace_id,
                commands=self._commands,
                response_size_limit_bytes=5,
            ),
        )
        self.assertEqual(
            {
                "__mcd_result_location__": "https://example.com/fizz_buzz",
                "__mcd_trace_id__": self._trace_id,
            },
            response.result,
        )
        mock_storage_client.write.assert_called_once_with(
            key=f"responses/{self._trace_id}",
            obj_to_write=json.dumps(
                {"__mcd_result__": {"fizz": "buzz"}, "__mcd_trace_id__": self._trace_id}
            ),
        )
        mock_storage_client.generate_presigned_url.assert_called_once_with(
            f"responses/{self._trace_id}", 3600
        )
