import base64
from unittest import TestCase

from apollo.credentials.base import BaseCredentialsService


class TestBaseCredentialsServiceDecode(TestCase):
    """Verify decode_dictionary runs after _merge_connect_args."""

    def test_plain_credentials_returned_unchanged(self):
        svc = BaseCredentialsService()
        creds = {"connect_args": {"host": "h", "port": 5432}}
        result = svc.get_credentials(creds)
        self.assertEqual({"connect_args": {"host": "h", "port": 5432}}, result)

    def test_binary_value_decoded(self):
        encoded = {"__type__": "bytes", "__data__": base64.b64encode(b"raw-cert").decode()}
        svc = BaseCredentialsService()
        result = svc.get_credentials({"connect_args": {"cert": encoded}})
        self.assertEqual(b"raw-cert", result["connect_args"]["cert"])


class TestBaseCredentialsServiceCcp(TestCase):
    """Verify CCP runs after decode when connection_type is provided."""

    def test_no_connection_type_skips_ccp(self):
        svc = BaseCredentialsService()
        flat = {"host": "h", "database": "d", "user": "u", "password": "p", "port": 5432}
        result = svc.get_credentials(flat)
        # No connection_type — CCP does not run, flat creds returned unchanged
        self.assertNotIn("connect_args", result)

    def test_postgres_flat_credentials_resolved(self):
        import apollo.integrations.ccp.defaults.postgres  # noqa: F401
        svc = BaseCredentialsService()
        result = svc.get_credentials(
            {
                "host": "db.example.com",
                "port": 5432,
                "database": "mydb",
                "user": "admin",
                "password": "secret",
            },
            connection_type="postgres",
        )
        self.assertIn("connect_args", result)
        self.assertEqual("db.example.com", result["connect_args"]["host"])
        self.assertEqual("mydb", result["connect_args"]["dbname"])
        self.assertNotIn("sslmode", result["connect_args"])
        self.assertNotIn("sslrootcert", result["connect_args"])

    def test_legacy_connect_args_not_overwritten_by_ccp(self):
        import apollo.integrations.ccp.defaults.postgres  # noqa: F401
        svc = BaseCredentialsService()
        # Legacy shape: connect_args already present — CCP is a no-op
        legacy = {"connect_args": {"host": "h", "dbname": "d"}}
        result = svc.get_credentials(legacy, connection_type="postgres")
        self.assertEqual(legacy, result)

    def test_unknown_connection_type_wraps_in_connect_args(self):
        svc = BaseCredentialsService()
        flat = {"host": "h", "database": "d"}
        result = svc.get_credentials(flat, connection_type="not_a_real_type")
        self.assertIn("connect_args", result)
        self.assertEqual("h", result["connect_args"]["host"])
        self.assertEqual("d", result["connect_args"]["database"])

    def test_legacy_connect_args_not_double_wrapped(self):
        svc = BaseCredentialsService()
        legacy = {"connect_args": {"host": "h"}}
        result = svc.get_credentials(legacy, connection_type="not_a_real_type")
        self.assertEqual(legacy, result)


class TestPassthroughCcp(TestCase):
    def test_passthrough_pipeline_returns_raw(self):
        from apollo.integrations.ccp.defaults.passthrough import PASSTHROUGH_CCP
        from apollo.integrations.ccp.pipeline import CcpPipeline

        raw = {"host": "h", "port": 5432, "user": "u", "password": "p"}
        result = CcpPipeline().execute(PASSTHROUGH_CCP, raw)
        self.assertEqual(raw, result)

    def test_passthrough_registered_connector_wraps_in_connect_args(self):
        from apollo.integrations.ccp.defaults.passthrough import PASSTHROUGH_CCP
        from apollo.integrations.ccp.registry import CcpRegistry

        CcpRegistry.register("_test_passthrough", PASSTHROUGH_CCP)
        result = CcpRegistry.resolve("_test_passthrough", {"host": "h", "port": 5432})
        self.assertIn("connect_args", result)
        self.assertEqual("h", result["connect_args"]["host"])
