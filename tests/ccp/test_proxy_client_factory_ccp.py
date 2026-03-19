from unittest import TestCase
from unittest.mock import patch

from apollo.agent.proxy_client_factory import ProxyClientFactory


class TestProxyClientFactoryCcp(TestCase):
    """Verify CCP is applied inside _create_proxy_client for registered types."""

    def test_postgres_flat_credentials_resolved_before_client_creation(self):
        flat = {
            "host": "db.example.com",
            "port": 5432,
            "database": "mydb",
            "user": "admin",
            "password": "secret",
        }
        captured = {}

        def fake_factory(credentials, platform):
            captured["credentials"] = credentials
            raise StopIteration  # bail out before actual connection

        with patch(
            "apollo.agent.proxy_client_factory._CLIENT_FACTORY_MAPPING",
            {"postgres": fake_factory},
        ):
            with self.assertRaises(StopIteration):
                ProxyClientFactory._create_proxy_client("postgres", flat, "local")

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
            {"postgres": fake_factory},
        ):
            with self.assertRaises(StopIteration):
                ProxyClientFactory._create_proxy_client("postgres", legacy, "local")

        self.assertEqual(legacy, captured["credentials"])
