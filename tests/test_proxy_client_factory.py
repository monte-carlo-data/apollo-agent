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

    @patch.object(Agent, "_execute_client_operation")
    @patch.object(ProxyClientFactory, "_create_proxy_client")
    def test_ctp_config_client_closed_even_when_not_skip_cache(
        self, mock_create_client, mock_execute
    ):
        # A custom ctp_config bypasses the cache, so the client is never cached
        # and must be closed in the finally even when skip_cache is False —
        # otherwise the temp files it registered linger until __del__/GC.
        mock_execute.return_value = AgentResponse({}, 200)
        client = SampleProxyClient()
        mock_create_client.return_value = client

        agent = Agent(LoggingUtils())
        agent.platform_provider = AwsPlatformProvider()  # caching platform
        operation = AgentCommands(trace_id="123", commands=[])  # skip_cache=False

        with patch.object(client, "close") as mock_close:
            agent.execute_operation(
                connection_type="test_type",
                operation_name="test_operation",
                operation_dict=operation.to_dict(),
                credentials=None,
                ctp_config={"mapper": {"field_map": {}}},
            )

        mock_close.assert_called_once()

    @patch("apollo.integrations.db.postgres_proxy_client.psycopg2.connect")
    def test_temp_files_cleaned_up_when_construction_fails(self, mock_connect):
        # If the pipeline materializes a CA file but the client constructor then
        # raises, nothing else would delete it — the factory must unlink it.
        import glob
        import os

        mock_connect.side_effect = RuntimeError("connect boom")
        creds = {
            "host": "h",
            "port": 5432,
            "database": "db",
            "user": "u",
            "password": "p",
            "ssl_options": {
                "ca_data": "-----BEGIN CERTIFICATE-----\nX\n-----END CERTIFICATE-----"
            },
        }
        before = set(glob.glob("/tmp/*_ssl_ca.pem"))
        with self.assertRaises(RuntimeError):
            ProxyClientFactory._create_proxy_client("postgres", creds, platform="test")
        after = set(glob.glob("/tmp/*_ssl_ca.pem"))
        self.assertEqual(before, after, "construction failure leaked a CA temp file")


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

    def test_mulesoft_factory_returns_mulesoft_subclass_with_platform_wired(self):
        # YET-1229: mulesoft routes through ``_get_proxy_client_mulesoft``
        # (not the generic ``_get_proxy_client_http`` it used pre-pivot)
        # and returns a ``MulesoftHttpProxyClient`` — the
        # ``HttpProxyClient`` subclass that adds
        # ``extract_mulesoft_sources``. The inherited HTTP surface stays
        # intact; the platform value still flows through to the
        # storage-factory default that ``download_to_storage`` uses for
        # other connectors.
        from apollo.agent.proxy_client_factory import (
            _CLIENT_FACTORY_MAPPING,
            _get_proxy_client_mulesoft,
        )
        from apollo.common.agent.constants import PLATFORM_AZURE
        from apollo.integrations.http.http_proxy_client import HttpProxyClient
        from apollo.integrations.http.mulesoft_proxy_client import (
            MulesoftHttpProxyClient,
        )

        self.assertIs(_CLIENT_FACTORY_MAPPING["mulesoft"], _get_proxy_client_mulesoft)
        client = _CLIENT_FACTORY_MAPPING["mulesoft"](
            {"connect_args": {}}, platform=PLATFORM_AZURE
        )
        self.assertIsInstance(client, MulesoftHttpProxyClient)
        self.assertIsInstance(client, HttpProxyClient)
        self.assertEqual(PLATFORM_AZURE, client._platform)
        # The MuleSoft-specific op is exposed + the inherited HTTP surface
        # is still callable. Together these guarantee the agent dispatch
        # can route both YET-1229's new op and YET-1130's existing REST
        # calls through the same client instance.
        self.assertTrue(hasattr(client, "extract_mulesoft_sources"))
        self.assertTrue(hasattr(client, "do_request"))
        self.assertTrue(hasattr(client, "download_bytes"))
