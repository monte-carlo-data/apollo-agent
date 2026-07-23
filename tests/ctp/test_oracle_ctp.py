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

    def test_ssl_options_carried_through_as_native_dict(self):
        """ssl_options must survive the pipeline as a dict (NativeEnvironment),
        not be stringified — OracleProxyClient reads ca_data/verify_identity etc.
        from it to build the wallet / ssl_context."""
        ssl_options = {
            "ca_data": "-----BEGIN CERTIFICATE-----X",
            "verify_identity": False,
        }
        result = CtpPipeline().execute(
            ORACLE_DEFAULT_CTP,
            {
                "dsn": "db.example.com:2484/ORCL",
                "user": "admin",
                "password": "secret",
                "ssl_options": ssl_options,
            },
        )
        self.assertEqual(ssl_options, result["ssl_options"])

    def test_resolve_keeps_ssl_options_in_connect_args(self):
        """End to end via resolve(): the DC sends ssl_options as a sibling of
        connect_args; it must land inside the resolved connect_args (SUP-style
        merge + passthrough) so the agent's OracleProxyClient receives it."""
        resolved = CtpRegistry.resolve(
            "oracle",
            {
                "connect_args": {
                    "dsn": "tcps://db.example.com:2484/ORCL",
                    "user": "admin",
                    "password": "secret",
                },
                "ssl_options": {"ca_data": "CA", "verify_identity": False},
            },
            temp_files=[],
        )
        self.assertEqual(
            {"ca_data": "CA", "verify_identity": False},
            resolved["connect_args"]["ssl_options"],
        )

    def test_no_ssl_options_absent_from_output(self):
        # With no ssl_options the mapper drops the (None) key, so connect_args has
        # no ssl_options — OracleProxyClient's pop(..., None) then falls back cleanly.
        result = CtpPipeline().execute(
            ORACLE_DEFAULT_CTP,
            {"dsn": "db.example.com:1521/ORCL", "user": "admin", "password": "secret"},
        )
        self.assertIsNone(result.get("ssl_options"))
