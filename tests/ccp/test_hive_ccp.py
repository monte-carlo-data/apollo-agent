from unittest import TestCase

from apollo.integrations.ccp.registry import CcpRegistry


class TestHiveCcp(TestCase):
    def test_hive_registered(self):
        config = CcpRegistry.get("hive")
        self.assertIsNotNone(config)
        self.assertEqual("hive-default", config.name)

    def test_resolve_flat_hive_credentials(self):
        result = CcpRegistry.resolve(
            "hive",
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
        self.assertIn("connect_args", result)
        args = result["connect_args"]
        self.assertEqual("localhost", args["host"])
        self.assertEqual(10000, args["port"])  # NativeEnvironment coerces "10000" → int
        self.assertEqual("foo", args["user"])
        self.assertEqual("fizz", args["database"])
        self.assertEqual("PLAIN", args["auth_mechanism"])
        self.assertEqual(870, args["timeout"])
        self.assertIs(False, args["use_ssl"])

    def test_resolve_omits_missing_optional_fields(self):
        result = CcpRegistry.resolve("hive", {"host": "localhost", "port": "10000"})
        args = result["connect_args"]
        self.assertNotIn("user", args)
        self.assertNotIn("database", args)
        self.assertNotIn("auth_mechanism", args)

    def test_resolve_legacy_hive_credentials_unchanged(self):
        legacy = {"connect_args": {"host": "h", "port": "10000"}}
        self.assertEqual(legacy, CcpRegistry.resolve("hive", legacy))
