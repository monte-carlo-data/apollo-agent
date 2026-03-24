from unittest import TestCase

from apollo.integrations.ccp.defaults.oracle import ORACLE_DEFAULT_CCP
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


class TestOracleCcp(TestCase):
    def test_not_registered(self):
        self.assertIsNone(CcpRegistry.get("oracle"))

    def test_resolve_flat_oracle_credentials(self):
        result = CcpPipeline().execute(
            ORACLE_DEFAULT_CCP,
            {
                "dsn": "db.example.com:1521/ORCL",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertEqual("db.example.com:1521/ORCL", result["dsn"])
        self.assertEqual("admin", result["user"])
        self.assertEqual("secret", result["password"])
        self.assertEqual(1, result["expire_time"])  # default applied by CCP

    def test_resolve_explicit_expire_time(self):
        result = CcpPipeline().execute(
            ORACLE_DEFAULT_CCP,
            {
                "dsn": "db.example.com:1521/ORCL",
                "user": "admin",
                "password": "secret",
                "expire_time": 5,
            },
        )
        self.assertEqual(5, result["expire_time"])
