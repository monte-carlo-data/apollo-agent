from unittest import TestCase
from unittest.mock import patch, call

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.common.agent.models import AgentCommands
from apollo.agent.proxy_client_factory import (
    ProxyClientFactory,
    get_native_connection_types,
)
from apollo.common.interfaces.agent_response import AgentResponse
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
        operation = AgentCommands(
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
        mock_create_client.assert_called_once_with(
            "test_type", None, agent.platform, ctp_config=None
        )

    @patch.object(Agent, "_execute_client_operation")
    @patch.object(ProxyClientFactory, "_create_proxy_client")
    def test_skip_cache_explicit(self, mock_create_client, mock_execute):
        mock_execute.return_value = AgentResponse({}, 200)
        mock_create_client.return_value = SampleProxyClient()

        agent = Agent(LoggingUtils())
        agent.platform_provider = AwsPlatformProvider()
        # skipping cache even if supported by platform
        operation = AgentCommands(
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
                call("test_type", None, agent.platform, ctp_config=None),
                call("test_type", None, agent.platform, ctp_config=None),
            ]
        )


class TestGetNativeConnectionTypes(TestCase):
    def test_returns_sorted_list(self):
        result = get_native_connection_types()
        self.assertEqual(result, sorted(result))

    def test_contains_known_types(self):
        result = get_native_connection_types()
        for expected in ["bigquery", "snowflake", "postgres", "redshift", "mysql"]:
            self.assertIn(expected, result)

    def test_returns_strings(self):
        result = get_native_connection_types()
        self.assertTrue(all(isinstance(t, str) for t in result))


class TestHttpProxyClientFactoryWiring(TestCase):
    """Lock in that `_get_proxy_client_http` forwards `platform` to
    `HttpProxyClient`. Required for `download_to_storage` to use the
    platform-default storage backend (S3/GCS/Azure) when MCD_STORAGE
    is unset — which is the production deployment shape."""

    def test_http_factory_forwards_platform_to_client(self):
        from apollo.agent.proxy_client_factory import _get_proxy_client_http
        from apollo.common.agent.constants import PLATFORM_AWS

        client = _get_proxy_client_http(
            credentials={"connect_args": {}}, platform=PLATFORM_AWS
        )
        self.assertEqual(PLATFORM_AWS, client._platform)

    def test_http_factory_forwards_platform_for_mulesoft(self):
        # mulesoft routes through the same _get_proxy_client_http; explicit
        # coverage that the dispatch keeps the platform wired.
        from apollo.agent.proxy_client_factory import (
            _CLIENT_FACTORY_MAPPING,
            _get_proxy_client_http,
        )
        from apollo.common.agent.constants import PLATFORM_AZURE

        self.assertIs(_CLIENT_FACTORY_MAPPING["mulesoft"], _get_proxy_client_http)
        client = _CLIENT_FACTORY_MAPPING["mulesoft"](
            {"connect_args": {}}, platform=PLATFORM_AZURE
        )
        self.assertEqual(PLATFORM_AZURE, client._platform)
