import gzip
import json
import os
from unittest import TestCase
from unittest.mock import (
    Mock,
    create_autospec,
    patch,
)

from apollo.agent.agent import Agent
from apollo.common.agent.env_vars import (
    PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS_ENV_VAR,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.common.agent.models import (
    AgentCommand,
    AgentCommands,
)
from apollo.integrations.storage.storage_proxy_client import StorageProxyClient
from tests.sample_proxy_client import SampleProxyClient


class AgentResponseTests(TestCase):
    def setUp(self):
        self._agent = Agent(LoggingUtils())
        self._client = SampleProxyClient()
        self._trace_id = "test_trace_id"
        self._commands = [AgentCommand(method="foo")]

    def test_no_pre_signed_urls(self):
        response = self._agent._execute_client_operation(
            connection_type="test",
            client=self._client,
            operation_name="test",
            operation=AgentCommands(
                trace_id=self._trace_id,
                commands=self._commands,
                response_size_limit_bytes=0,
            ),
            func=lambda client: {"foo": "bar"},
        )
        self.assertEqual(
            {"__mcd_result__": {"foo": "bar"}, "__mcd_trace_id__": self._trace_id},
            response.result,
        )

    @patch("apollo.agent.agent.StorageProxyClient")
    def test_use_pre_signed_urls(self, storage_mock: Mock):
        expected_expiration = 50
        mock_storage_client = create_autospec(StorageProxyClient)
        storage_mock.return_value = mock_storage_client
        mock_storage_client.write.return_value = None
        mock_storage_client.generate_presigned_url.return_value = (
            "https://example.com/fizz_buzz"
        )
        with patch.dict(
            os.environ,
            {
                PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS_ENV_VAR: str(
                    expected_expiration
                ),
            },
        ):
            response = self._agent._execute_client_operation(
                connection_type="test",
                client=self._client,
                operation_name="test",
                operation=AgentCommands(
                    trace_id=self._trace_id,
                    commands=self._commands,
                    response_size_limit_bytes=5,
                ),
                func=lambda client: {"fizz": "buzz"},
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
            f"responses/{self._trace_id}", expected_expiration
        )

    @patch("apollo.agent.agent.StorageProxyClient")
    def test_use_pre_signed_url_compressed(self, storage_mock: Mock):
        expected_expiration = 50
        mock_storage_client = create_autospec(StorageProxyClient)
        storage_mock.return_value = mock_storage_client
        mock_storage_client.write.return_value = None
        mock_storage_client.generate_presigned_url.return_value = (
            "https://example.com/fizz_buzz"
        )
        with patch.dict(
            os.environ,
            {
                PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS_ENV_VAR: str(
                    expected_expiration
                ),
            },
        ):
            response = self._agent._execute_client_operation(
                connection_type="test",
                client=self._client,
                operation_name="test",
                operation=AgentCommands(
                    trace_id=self._trace_id,
                    commands=self._commands,
                    response_size_limit_bytes=5,
                    compress_response_file=True,
                ),
                func=lambda client: {"fizz": "buzz"},
            )
        self.assertEqual(
            {
                "__mcd_result_location__": "https://example.com/fizz_buzz",
                "__mcd_trace_id__": self._trace_id,
                "__mcd_result_compressed__": True,
            },
            response.result,
        )
        expected_result = json.dumps(
            {"__mcd_result__": {"fizz": "buzz"}, "__mcd_trace_id__": self._trace_id}
        )
        mock_storage_client.write.assert_called_once_with(
            key=f"responses/{self._trace_id}",
            obj_to_write=gzip.compress(expected_result.encode("utf-8")),
        )
        mock_storage_client.generate_presigned_url.assert_called_once_with(
            f"responses/{self._trace_id}", expected_expiration
        )

    def test_no_pre_signed_urls_compressed(self):
        response = self._agent._execute_client_operation(
            connection_type="test",
            client=self._client,
            operation_name="test",
            operation=AgentCommands(
                trace_id=self._trace_id,
                commands=self._commands,
                response_size_limit_bytes=0,
                compress_response_threshold_bytes=5,
            ),
            func=lambda client: {"foo": "bar"},
        )
        expected_result = {
            "__mcd_result__": {"foo": "bar"},
            "__mcd_trace_id__": self._trace_id,
        }
        self.assertEqual(
            gzip.compress(json.dumps(expected_result).encode("utf-8")),
            response.result,
        )
        self.assertTrue(response.compressed)
