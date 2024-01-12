from unittest import TestCase
from unittest.mock import patch, call

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.models import AgentOperation
from apollo.agent.proxy_client_factory import ProxyClientFactory
from apollo.interfaces.agent_response import AgentResponse
from apollo.interfaces.azure.azure_platform import AzurePlatformProvider
from apollo.interfaces.lambda_function.platform import AwsPlatformProvider
from tests.sample_proxy_client import SampleProxyClient


class ProxyClientFactoryTests(TestCase):
    @patch.object(Agent, "_execute_client_operation")
    @patch.object(ProxyClientFactory, "_create_proxy_client")
    def test_cached_client(self, mock_create_client, mock_execute):
        mock_execute.return_value = AgentResponse({}, 200)
        mock_create_client.return_value = SampleProxyClient()

        agent = Agent(LoggingUtils())
        agent.platform_provider = AwsPlatformProvider()
        # AWS and GCP support caching, so it will be used by default
        operation = AgentOperation(
            trace_id="123",
            commands=[],
        )
        agent.execute_operation(
            connection_type="test_type",
            operation_name="test_operation",
            operation_dict=operation.to_dict(),
            credentials=None,
        )
        agent.execute_operation(
            connection_type="test_type",
            operation_name="test_operation",
            operation_dict=operation.to_dict(),
            credentials=None,
        )
        # two invocations, a single client created
        mock_create_client.assert_called_once_with("test_type", None, agent.platform)

    @patch.object(Agent, "_execute_client_operation")
    @patch.object(ProxyClientFactory, "_create_proxy_client")
    def test_skip_cache_explicit(self, mock_create_client, mock_execute):
        mock_execute.return_value = AgentResponse({}, 200)
        mock_create_client.return_value = SampleProxyClient()

        agent = Agent(LoggingUtils())
        agent.platform_provider = AwsPlatformProvider()
        # skipping cache even if supported by platform
        operation = AgentOperation(
            trace_id="123",
            commands=[],
            skip_cache=True,
        )
        agent.execute_operation(
            connection_type="test_type",
            operation_name="test_operation",
            operation_dict=operation.to_dict(),
            credentials=None,
        )
        agent.execute_operation(
            connection_type="test_type",
            operation_name="test_operation",
            operation_dict=operation.to_dict(),
            credentials=None,
        )
        # two invocations, two clients created
        mock_create_client.assert_has_calls(
            [
                call("test_type", None, agent.platform),
                call("test_type", None, agent.platform),
            ]
        )

    @patch.object(Agent, "_execute_client_operation")
    @patch.object(ProxyClientFactory, "_create_proxy_client")
    def test_skip_cache_platform(self, mock_create_client, mock_execute):
        mock_execute.return_value = AgentResponse({}, 200)
        mock_create_client.return_value = SampleProxyClient()

        agent = Agent(LoggingUtils())
        agent.platform_provider = AzurePlatformProvider()
        # Azure doesn't support caching, so it won't be used
        operation = AgentOperation(
            trace_id="123",
            commands=[],
        )
        agent.execute_operation(
            connection_type="test_type",
            operation_name="test_operation",
            operation_dict=operation.to_dict(),
            credentials=None,
        )
        agent.execute_operation(
            connection_type="test_type",
            operation_name="test_operation",
            operation_dict=operation.to_dict(),
            credentials=None,
        )
        # two invocations, two clients created
        mock_create_client.assert_has_calls(
            [
                call("test_type", None, agent.platform),
                call("test_type", None, agent.platform),
            ]
        )
