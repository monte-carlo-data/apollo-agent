import os
from unittest import TestCase
from unittest.mock import patch

from apollo.integrations.ccp.defaults.mysql import MYSQL_DEFAULT_CCP
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


class TestMysqlCcp(TestCase):
    def test_not_registered(self):
        self.assertIsNone(CcpRegistry.get("mysql"))

    def test_resolve_flat_mysql_no_ssl(self):
        result = CcpPipeline().execute(
            MYSQL_DEFAULT_CCP,
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

    @patch("apollo.integrations.ccp.transforms.fetch_remote_file.urlretrieve")
    def test_resolve_mysql_remote_ca_url(self, mock_urlretrieve):
        """ssl_options.ca (URL) triggers fetch_remote_file; ssl={"ca": path}."""

        def fake_retrieve(url, filename):
            with open(filename, "w") as f:
                f.write("FAKE CERT")

        mock_urlretrieve.side_effect = fake_retrieve

        result = CcpPipeline().execute(
            MYSQL_DEFAULT_CCP,
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

        result = CcpPipeline().execute(
            MYSQL_DEFAULT_CCP,
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
