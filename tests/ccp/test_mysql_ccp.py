import os
from unittest import TestCase
from unittest.mock import patch

from apollo.integrations.ccp.registry import CcpRegistry


class TestMysqlCcp(TestCase):
    def test_mysql_registered(self):
        config = CcpRegistry.get("mysql")
        self.assertIsNotNone(config)
        self.assertEqual("mysql-default", config.name)

    def test_resolve_flat_mysql_no_ssl(self):
        result = CcpRegistry.resolve(
            "mysql",
            {
                "host": "db.example.com",
                "port": "3306",
                "user": "admin",
                "password": "secret",
            },
        )
        self.assertIn("connect_args", result)
        args = result["connect_args"]
        self.assertEqual("db.example.com", args["host"])
        self.assertEqual(3306, args["port"])  # NativeEnvironment coerces "3306" → int
        self.assertEqual("admin", args["user"])
        self.assertEqual("secret", args["password"])
        self.assertNotIn("ssl", args)

    @patch("apollo.integrations.ccp.transforms.fetch_remote_file.urlretrieve")
    def test_resolve_mysql_remote_ca_url(self, mock_urlretrieve):
        """ssl_options.ca (URL) triggers fetch_remote_file; ssl={"ca": path}."""

        def fake_retrieve(url, filename):
            with open(filename, "w") as f:
                f.write("FAKE CERT")

        mock_urlretrieve.side_effect = fake_retrieve

        result = CcpRegistry.resolve(
            "mysql",
            {
                "host": "db.example.com",
                "port": "3306",
                "user": "admin",
                "password": "secret",
                "ssl_options": {"ca": "https://certs.example.com/ca.pem"},
            },
        )

        args = result["connect_args"]
        self.assertIn("ssl", args)
        ssl_arg = args["ssl"]
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

        result = CcpRegistry.resolve(
            "mysql",
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

        args = result["connect_args"]
        self.assertIn("ssl", args)
        self.assertIsInstance(args["ssl"], ssl.SSLContext)

    def test_resolve_legacy_mysql_credentials_unchanged(self):
        legacy = {
            "connect_args": {"host": "h", "port": "3306", "user": "u", "password": "p"}
        }
        self.assertEqual(legacy, CcpRegistry.resolve("mysql", legacy))
