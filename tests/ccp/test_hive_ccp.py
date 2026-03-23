from unittest import TestCase

from apollo.integrations.ccp.defaults.hive import HIVE_DEFAULT_CCP
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


class TestHiveCcp(TestCase):
    def test_not_registered(self):
        self.assertIsNone(CcpRegistry.get("hive"))

    def test_resolve_flat_hive_credentials(self):
        result = CcpPipeline().execute(
            HIVE_DEFAULT_CCP,
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
        result = CcpPipeline().execute(
            HIVE_DEFAULT_CCP, {"host": "localhost", "port": "10000"}
        )
        self.assertNotIn("user", result)
        self.assertNotIn("database", result)
        self.assertNotIn("auth_mechanism", result)
