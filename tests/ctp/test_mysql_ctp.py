import os
import ssl as ssl_module
from unittest import TestCase
from unittest.mock import patch

from apollo.integrations.ctp.defaults.mysql import MYSQL_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


class TestMysqlCtp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CtpRegistry.get("mysql"))

    def test_resolve_flat_mysql_no_ssl(self):
        result = CtpPipeline().execute(
            MYSQL_DEFAULT_CTP,
            {
                "host": "db.example.com",
                "port": "3306",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertEqual("db.example.com", result["host"])
        self.assertEqual(3306, result["port"])  # NativeEnvironment coerces "3306" → int
        self.assertEqual("admin", result["user"])
        self.assertEqual("secret", result["password"])
        self.assertNotIn("ssl", result)

    @patch("apollo.integrations.ctp.transforms.fetch_remote_file.urlretrieve")
    def test_resolve_mysql_remote_ca_url(self, mock_urlretrieve):
        """ssl_options.ca (URL) triggers fetch_remote_file; ssl={"ca": path}."""

        def fake_retrieve(url, filename):
            with open(filename, "w") as f:
                f.write("FAKE CERT")

        mock_urlretrieve.side_effect = fake_retrieve

        result = CtpPipeline().execute(
            MYSQL_DEFAULT_CTP,
            {
                "host": "db.example.com",
                "port": "3306",
                "user": "admin",
                "password": "secret",
                "ssl_options": {"ca": "https://certs.example.com/ca.pem"},
            },
        )

        self.assertIn("ssl", result)
        ssl_arg = result["ssl"]
        self.assertIsInstance(ssl_arg, dict)
        self.assertIn("ca", ssl_arg)
        cert_path = ssl_arg["ca"]
        self.assertTrue(os.path.exists(cert_path))
        os.unlink(cert_path)

        mock_urlretrieve.assert_called_once()

    @patch("ssl.SSLContext.load_verify_locations")
    def test_resolve_mysql_inline_ca_data(self, _mock_load):
        """ssl_options with ca_data (no ca URL) triggers resolve_ssl_options; ssl=SSLContext."""
        import ssl

        result = CtpPipeline().execute(
            MYSQL_DEFAULT_CTP,
            {
                "host": "db.example.com",
                "port": "3306",
                "user": "admin",
                "password": "secret",
                "ssl_options": {
                    "ca_data": "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----"
                },
            },
        )

        self.assertIn("ssl", result)
        self.assertIsInstance(result["ssl"], ssl.SSLContext)

    @patch("ssl.SSLContext.load_verify_locations")
    def test_resolve_mysql_dc_pre_shaped_credentials_with_outer_ssl_options(
        self, _mock_load
    ):
        """SUP-373: this is the credentials shape the data-collector actually sends to apollo
        for the mysql agent path (see DC clients/plugins/plugin_mysql.py:48-51):

            {
                "connect_args": {host, port, user, password},   # wrapped
                "ssl_options": {"ca_data": "..."},                # OUTER level, sibling to connect_args
            }

        CtpRegistry.resolve unwraps connect_args (registry.py:91, 126) and runs the
        pipeline with only the inner dict. If this test fails, ssl_options at the outer
        level is discarded by the unwrap — the mysql CTP's `when="raw.ssl_options is
        defined"` doesn't fire, connect_args["ssl"] never gets set, pymysql connects
        without TLS, and MySQL with require_secure_transport=ON returns error 3159.

        This is PennyMac's failure mode.
        """
        dc_pre_shaped_credentials = {
            "connect_args": {
                "host": "db.example.com",
                "port": 3306,
                "user": "admin",
                "password": "secret",
            },
            "ssl_options": {
                "ca_data": "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----"
            },
        }

        resolved = CtpRegistry.resolve("mysql", dc_pre_shaped_credentials)

        # The post-resolve credentials must contain a usable ssl context for pymysql.
        # If the unwrap discarded the outer ssl_options, this assertion will fail.
        self.assertIn("connect_args", resolved)
        connect_args = resolved["connect_args"]
        self.assertIn(
            "ssl",
            connect_args,
            "SUP-373: outer ssl_options got dropped by CtpRegistry.resolve's "
            "connect_args unwrap. The DC sends ssl_options as a SIBLING of "
            "connect_args, not nested inside it. The unwrap at registry.py:91/126 "
            "discards everything outside connect_args before the pipeline runs.",
        )
        self.assertIsInstance(
            connect_args["ssl"],
            ssl_module.SSLContext,
            f"ssl is set but not an SSLContext: {connect_args['ssl']!r}",
        )

        # F8 follow-up: explicitly verify inner connect_args fields survive the merge.
        # Without these assertions a regression that drops the inner dict would still
        # pass the SSL check above (ssl_options would still merge in from outer).
        self.assertEqual("db.example.com", connect_args["host"])
        self.assertEqual(3306, connect_args["port"])
        self.assertEqual("admin", connect_args["user"])
        self.assertEqual("secret", connect_args["password"])
