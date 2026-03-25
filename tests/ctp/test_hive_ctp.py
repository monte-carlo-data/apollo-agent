from unittest import TestCase

from apollo.integrations.ctp.defaults.hive import HIVE_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


class TestHiveCtp(TestCase):
    def test_not_registered(self):
        self.assertIsNone(CtpRegistry.get("hive"))

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
