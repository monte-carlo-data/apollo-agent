from unittest import TestCase

from apollo.integrations.ctp.defaults.oracle import ORACLE_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


class TestOracleCtp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CtpRegistry.get("oracle"))

    def test_resolve_flat_oracle_credentials(self):
        result = CtpPipeline().execute(
            ORACLE_DEFAULT_CTP,
            {
                "dsn": "db.example.com:1521/ORCL",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertEqual("db.example.com:1521/ORCL", result["dsn"])
        self.assertEqual("admin", result["user"])
        self.assertEqual("secret", result["password"])
        self.assertEqual(1, result["expire_time"])  # default applied by CTP

    def test_resolve_explicit_expire_time(self):
        result = CtpPipeline().execute(
            ORACLE_DEFAULT_CTP,
            {
                "dsn": "db.example.com:1521/ORCL",
                "user": "admin",
                "password": "secret",
                "expire_time": 5,
            },
        )
        self.assertEqual(5, result["expire_time"])

    def test_thick_mode_absent_by_default(self):
        result = CtpPipeline().execute(
            ORACLE_DEFAULT_CTP,
            {
                "dsn": "db.example.com:1521/ORCL",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertNotIn("thick_mode", result)

    def test_thick_mode_preserved_as_bool(self):
        result = CtpPipeline().execute(
            ORACLE_DEFAULT_CTP,
            {
                "dsn": "db.example.com:1521/ORCL",
                "user": "admin",
                "password": "secret",
                "thick_mode": True,
            },
        )
        # NativeEnvironment keeps the bool a bool — critical, since a "False"
        # string would be truthy and silently force thick mode on.
        self.assertIs(True, result["thick_mode"])
