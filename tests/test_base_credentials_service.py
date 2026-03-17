import base64
from unittest import TestCase

from apollo.credentials.base import BaseCredentialsService


class TestBaseCredentialsServiceDecode(TestCase):
    """Verify decode_dictionary runs after _merge_connect_args."""

    def test_plain_credentials_returned_unchanged(self):
        svc = BaseCredentialsService()
        creds = {"connect_args": {"host": "h", "port": 5432}}
        result = svc.get_credentials(creds)
        self.assertEqual({"connect_args": {"host": "h", "port": 5432}}, result)


class TestBaseCredentialsServiceCcp(TestCase):
    """Verify CCP runs after decode when connection_type is provided."""

    def test_no_connection_type_skips_ccp(self):
        svc = BaseCredentialsService()
        flat = {"host": "h", "database": "d", "user": "u", "password": "p", "port": 5432}
        result = svc.get_credentials(flat)
        # No connection_type — CCP does not run, flat creds returned unchanged
        self.assertNotIn("connect_args", result)

    def test_postgres_flat_credentials_resolved(self):
        import apollo.integrations.ccp.defaults.postgres  # noqa: F401
        svc = BaseCredentialsService()
        result = svc.get_credentials(
            {
                "host": "db.example.com",
                "port": 5432,
                "database": "mydb",
                "user": "admin",
                "password": "secret",
            },
            connection_type="postgres",
        )
        self.assertIn("connect_args", result)
        self.assertEqual("db.example.com", result["connect_args"]["host"])
        self.assertEqual("mydb", result["connect_args"]["dbname"])
        self.assertEqual("require", result["connect_args"]["sslmode"])
        self.assertNotIn("sslrootcert", result["connect_args"])

    def test_legacy_connect_args_not_overwritten_by_ccp(self):
        import apollo.integrations.ccp.defaults.postgres  # noqa: F401
        svc = BaseCredentialsService()
        # Legacy shape: connect_args already present — CCP is a no-op
        legacy = {"connect_args": {"host": "h", "dbname": "d"}}
        result = svc.get_credentials(legacy, connection_type="postgres")
        self.assertEqual(legacy, result)

    def test_unknown_connection_type_returns_credentials_unchanged(self):
        svc = BaseCredentialsService()
        flat = {"host": "h", "database": "d"}
        result = svc.get_credentials(flat, connection_type="not_a_real_type")
        self.assertEqual(flat, result)

    def test_encoded_bytes_decoded_before_ccp_runs(self):
        """decode_bytes transform runs before other CCP steps, so encoded values are resolved."""
        import apollo.integrations.ccp.defaults.postgres  # noqa: F401
        encoded_pem = {"__type__": "bytes", "__data__": base64.b64encode(b"PEM_CONTENT").decode()}
        svc = BaseCredentialsService()
        result = svc.get_credentials(
            {
                "host": "db.example.com",
                "port": 5432,
                "database": "mydb",
                "user": "admin",
                "password": "secret",
                "ssl_ca_pem": encoded_pem,
            },
            connection_type="postgres",
        )
        self.assertIn("connect_args", result)
        # tmp_file_write ran — the decoded PEM was written to a temp file
        self.assertIn("sslrootcert", result["connect_args"])
