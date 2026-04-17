from unittest import TestCase
from unittest.mock import patch

from apollo.agent.proxy_client_factory import ProxyClientFactory
from apollo.integrations.ctp.defaults.informatica import INFORMATICA_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


class TestInformaticaCtpRegistered(TestCase):
    def test_registered(self):
        """CtpRegistry.get("informatica") must return a config — not None.

        If this fails, the import inside _discover() is missing from registry.py.
        """
        self.assertIsNotNone(CtpRegistry.get("informatica"))


class TestInformaticaCtpMapper(TestCase):
    """Verify the default mapper produces the correct connect_args shape."""

    def test_resolve_v2_flat_credentials(self):
        """V2 flat credentials are mapped to expected connect_args keys."""
        result = CtpPipeline().execute(
            INFORMATICA_DEFAULT_CTP,
            {
                "username": "svc_user",
                "password": "s3cr3t",
                "informatica_auth": "v2",
                "base_url": "https://dm-eu.informaticacloud.com",
            },
        )
        self.assertEqual("svc_user", result["username"])
        self.assertEqual("s3cr3t", result["password"])
        self.assertEqual("v2", result["informatica_auth"])
        self.assertEqual("https://dm-eu.informaticacloud.com", result["base_url"])

    def test_resolve_v3_flat_credentials(self):
        """V3 flat credentials are mapped; informatica_auth and base_url present."""
        result = CtpPipeline().execute(
            INFORMATICA_DEFAULT_CTP,
            {
                "username": "svc_user",
                "password": "s3cr3t",
                "informatica_auth": "v3",
            },
        )
        self.assertEqual("svc_user", result["username"])
        self.assertEqual("s3cr3t", result["password"])
        self.assertEqual("v3", result["informatica_auth"])

    def test_resolve_minimal_credentials(self):
        """Only username and password are required; optional fields absent when not provided."""
        result = CtpPipeline().execute(
            INFORMATICA_DEFAULT_CTP,
            {
                "username": "svc_user",
                "password": "s3cr3t",
            },
        )
        self.assertEqual("svc_user", result["username"])
        self.assertEqual("s3cr3t", result["password"])
        # Optional fields absent (None values are filtered by the mapper)
        self.assertNotIn("informatica_auth", result)
        self.assertNotIn("base_url", result)


class TestInformaticaCtpFactoryResolution(TestCase):
    """Verify CTP pipeline runs before InformaticaProxyClient is instantiated."""

    def test_flat_credentials_resolved_to_connect_args_before_client_creation(self):
        """Flat credentials are wrapped in connect_args by CTP before the factory is called."""
        flat = {
            "username": "svc_user",
            "password": "s3cr3t",
            "informatica_auth": "v3",
        }
        captured = {}

        def fake_factory(credentials, **kwargs):
            captured["credentials"] = credentials
            raise StopIteration  # bail before attempting real network calls

        with patch(
            "apollo.agent.proxy_client_factory._CLIENT_FACTORY_MAPPING",
            {"informatica": fake_factory},
        ):
            with self.assertRaises(StopIteration):
                ProxyClientFactory._create_proxy_client("informatica", flat, "local")

        self.assertIn("connect_args", captured["credentials"])
        connect_args = captured["credentials"]["connect_args"]
        self.assertEqual("svc_user", connect_args["username"])
        self.assertEqual("s3cr3t", connect_args["password"])
        self.assertEqual("v3", connect_args["informatica_auth"])
