from typing import Required, TypedDict
from unittest import TestCase
from unittest.mock import patch

from apollo.agent.proxy_client_factory import ProxyClientFactory
from apollo.integrations.ccp.models import CcpConfig, MapperConfig
from apollo.integrations.ccp.registry import CcpRegistry

_TEST_CONNECTION_TYPE = "test-ccp-factory"


class _TestClientArgs(TypedDict):
    host: Required[str]
    dbname: Required[str]


_TEST_CCP_CONFIG = CcpConfig(
    name="test-ccp-factory-default",
    steps=[],
    mapper=MapperConfig(
        name="test_client_args",
        schema=_TestClientArgs,
        field_map={
            "host": "{{ raw.host }}",
            "dbname": "{{ raw.database }}",
        },
    ),
)


class TestProxyClientFactoryCcp(TestCase):
    """Verify CCP is applied inside _create_proxy_client for registered types."""

    def setUp(self):
        CcpRegistry.register(_TEST_CONNECTION_TYPE, _TEST_CCP_CONFIG)

    def tearDown(self):
        CcpRegistry._registry.pop(_TEST_CONNECTION_TYPE, None)

    def test_flat_credentials_resolved_before_client_creation(self):
        flat = {"host": "db.example.com", "database": "mydb"}
        captured = {}

        def fake_factory(credentials, platform):
            captured["credentials"] = credentials
            raise StopIteration  # bail out before actual connection

        with patch(
            "apollo.agent.proxy_client_factory._CLIENT_FACTORY_MAPPING",
            {_TEST_CONNECTION_TYPE: fake_factory},
        ):
            with self.assertRaises(StopIteration):
                ProxyClientFactory._create_proxy_client(
                    _TEST_CONNECTION_TYPE, flat, "local"
                )

        self.assertIn("connect_args", captured["credentials"])
        self.assertEqual(
            "db.example.com", captured["credentials"]["connect_args"]["host"]
        )
        self.assertEqual("mydb", captured["credentials"]["connect_args"]["dbname"])

    def test_http_credentials_pass_through_unchanged(self):
        http_creds = {"token": "Bearer abc123"}
        captured = {}

        def fake_factory(credentials, platform):
            captured["credentials"] = credentials
            raise StopIteration

        with patch(
            "apollo.agent.proxy_client_factory._CLIENT_FACTORY_MAPPING",
            {"http": fake_factory},
        ):
            with self.assertRaises(StopIteration):
                ProxyClientFactory._create_proxy_client("http", http_creds, "local")

        # http is not in the CCP registry — credentials must NOT be wrapped
        self.assertNotIn("connect_args", captured["credentials"])
        self.assertEqual("Bearer abc123", captured["credentials"]["token"])

    def test_legacy_connect_args_not_overwritten_by_ccp(self):
        legacy = {"connect_args": {"host": "h", "dbname": "d"}}
        captured = {}

        def fake_factory(credentials, platform):
            captured["credentials"] = credentials
            raise StopIteration

        with patch(
            "apollo.agent.proxy_client_factory._CLIENT_FACTORY_MAPPING",
            {_TEST_CONNECTION_TYPE: fake_factory},
        ):
            with self.assertRaises(StopIteration):
                ProxyClientFactory._create_proxy_client(
                    _TEST_CONNECTION_TYPE, legacy, "local"
                )

        self.assertEqual(legacy, captured["credentials"])
