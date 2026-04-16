from unittest import TestCase

from apollo.integrations.ctp.defaults.hive import HIVE_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


class TestHiveCtp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CtpRegistry.get("hive"))

    def test_resolve_flat_hive_credentials(self):
        result = CtpPipeline().execute(
            HIVE_DEFAULT_CTP,
            {
                "host": "localhost",
                "port": "10000",
                "user": "foo",
                "database": "fizz",
                "auth_mechanism": "PLAIN",
                "timeout": 870,
                "use_ssl": False,
            },
        )
        self.assertEqual("localhost", result["host"])
        self.assertEqual(
            10000, result["port"]
        )  # NativeEnvironment coerces "10000" → int
        self.assertEqual("foo", result["user"])
        self.assertEqual("fizz", result["database"])
        self.assertEqual("PLAIN", result["auth_mechanism"])
        self.assertEqual(870, result["timeout"])
        self.assertIs(False, result["use_ssl"])

    def test_resolve_omits_missing_optional_fields(self):
        result = CtpPipeline().execute(
            HIVE_DEFAULT_CTP, {"host": "localhost", "port": "10000"}
        )
        self.assertNotIn("user", result)
        self.assertNotIn("database", result)
        self.assertNotIn("auth_mechanism", result)
        self.assertNotIn("use_http_transport", result)
        self.assertNotIn("http_path", result)
        self.assertNotIn("kerberos_service_name", result)

    def test_resolve_http_mode_credentials(self):
        """HTTP/Databricks mode fields must survive the CTP pipeline."""
        result = CtpPipeline().execute(
            HIVE_DEFAULT_CTP,
            {
                "host": "workspace.cloud.databricks.com",
                "port": 443,
                "use_http_transport": True,
                "http_path": "sql/protocolv1/o/123456/cluster-id",
                "use_ssl": True,
                "user": "token",
                "password": "dapi-secret",
                "auth_mechanism": "PLAIN",
                "timeout": 870,
            },
        )
        self.assertEqual("workspace.cloud.databricks.com", result["host"])
        self.assertEqual(443, result["port"])
        self.assertIs(True, result["use_http_transport"])
        self.assertEqual("sql/protocolv1/o/123456/cluster-id", result["http_path"])
        self.assertIs(True, result["use_ssl"])
        self.assertEqual("token", result["user"])
        self.assertEqual("dapi-secret", result["password"])
        self.assertEqual("PLAIN", result["auth_mechanism"])

    def test_resolve_kerberos_credentials(self):
        result = CtpPipeline().execute(
            HIVE_DEFAULT_CTP,
            {
                "host": "hive-server",
                "port": 10000,
                "auth_mechanism": "GSSAPI",
                "kerberos_service_name": "hive",
            },
        )
        self.assertEqual("hive-server", result["host"])
        self.assertEqual("GSSAPI", result["auth_mechanism"])
        self.assertEqual("hive", result["kerberos_service_name"])
