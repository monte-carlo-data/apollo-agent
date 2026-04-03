from typing import Required, TypedDict
from unittest import TestCase
from unittest.mock import patch

from apollo.agent.proxy_client_factory import ProxyClientFactory
from apollo.integrations.ctp.models import CtpConfig, MapperConfig
from apollo.integrations.ctp.registry import CtpRegistry

_TEST_CONNECTION_TYPE = "test-ctp-factory"


class _TestClientArgs(TypedDict):
    host: Required[str]
    dbname: Required[str]


_TEST_CTP_CONFIG = CtpConfig(
    name="test-ctp-factory-default",
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


class TestProxyClientFactoryCtp(TestCase):
    """Verify CTP is applied inside _create_proxy_client for registered types."""

    def setUp(self):
        CtpRegistry.register(_TEST_CONNECTION_TYPE, _TEST_CTP_CONFIG)

    def tearDown(self):
        CtpRegistry._registry.pop(_TEST_CONNECTION_TYPE, None)

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

    def test_http_credentials_run_through_ctp(self):
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

        # http is registered in CTP — credentials are wrapped in connect_args
        self.assertIn("connect_args", captured["credentials"])
        self.assertEqual(
            "Bearer abc123", captured["credentials"]["connect_args"]["token"]
        )

    def test_dc_shaped_credentials_run_through_ctp(self):
        # DC pre-shapes credentials into connect_args before calling the agent.
        # CTP unwraps the inner dict and runs it through the pipeline, so
        # field aliases (e.g. database → dbname) are normalised on both paths.
        dc_shaped = {"connect_args": {"host": "db.example.com", "database": "mydb"}}
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
                    _TEST_CONNECTION_TYPE, dc_shaped, "local"
                )

        self.assertIn("connect_args", captured["credentials"])
        ca = captured["credentials"]["connect_args"]
        self.assertEqual("db.example.com", ca["host"])
        self.assertEqual("mydb", ca["dbname"])
