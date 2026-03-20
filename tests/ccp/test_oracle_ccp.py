from unittest import TestCase

from apollo.integrations.ccp.registry import CcpRegistry


class TestOracleCcp(TestCase):
    def test_oracle_registered(self):
        config = CcpRegistry.get("oracle")
        self.assertIsNotNone(config)
        self.assertEqual("oracle-default", config.name)

    def test_resolve_flat_oracle_credentials(self):
        result = CcpRegistry.resolve(
            "oracle",
            {
                "dsn": "db.example.com:1521/ORCL",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertIn("connect_args", result)
        args = result["connect_args"]
        self.assertEqual("db.example.com:1521/ORCL", args["dsn"])
        self.assertEqual("admin", args["user"])
        self.assertEqual("secret", args["password"])
        self.assertEqual(1, args["expire_time"])  # default applied by CCP

    def test_resolve_explicit_expire_time(self):
        result = CcpRegistry.resolve(
            "oracle",
            {
                "dsn": "db.example.com:1521/ORCL",
                "user": "admin",
                "password": "secret",
                "expire_time": 5,
            },
        )
        self.assertEqual(5, result["connect_args"]["expire_time"])

    def test_resolve_legacy_oracle_credentials_unchanged(self):
        legacy = {"connect_args": {"dsn": "h:1521/DB", "user": "u", "password": "p"}}
        self.assertEqual(legacy, CcpRegistry.resolve("oracle", legacy))
