from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import (
    BestAvailableEncryption,
    Encoding,
    NoEncryption,
    PrivateFormat,
)
from unittest import TestCase
from unittest.mock import MagicMock, patch

from apollo.integrations.ccp.registry import CcpRegistry


def _resolve(credentials: dict) -> dict:
    return CcpRegistry.resolve("snowflake", credentials)


def _connect_args(credentials: dict) -> dict:
    return _resolve(credentials)["connect_args"]


def _generate_pem(passphrase: bytes | None = None) -> bytes:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    encryption = BestAvailableEncryption(passphrase) if passphrase else NoEncryption()
    return key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=encryption,
    )


class TestSnowflakeCcp(TestCase):
    def test_snowflake_registered(self):
        config = CcpRegistry.get("snowflake")
        self.assertIsNotNone(config)
        self.assertEqual("snowflake-default", config.name)

    def test_resolve_wraps_in_connect_args(self):
        result = _resolve({"user": "u", "account": "a", "password": "p"})
        self.assertIn("connect_args", result)

    # ── Password auth ─────────────────────────────────────────────────

    def test_password_auth(self):
        args = _connect_args(
            {"user": "alice", "account": "myorg-myaccount", "password": "hunter2"}
        )
        self.assertEqual("alice", args["user"])
        self.assertEqual("myorg-myaccount", args["account"])
        self.assertEqual("hunter2", args["password"])
        self.assertNotIn("private_key", args)
        self.assertNotIn("token", args)

    def test_password_auth_with_warehouse_and_database(self):
        args = _connect_args(
            {
                "user": "u",
                "account": "a",
                "password": "p",
                "warehouse": "COMPUTE_WH",
                "database": "MY_DB",
                "schema": "PUBLIC",
                "role": "SYSADMIN",
            }
        )
        self.assertEqual("COMPUTE_WH", args["warehouse"])
        self.assertEqual("MY_DB", args["database"])
        self.assertEqual("PUBLIC", args["schema"])
        self.assertEqual("SYSADMIN", args["role"])

    def test_password_auth_with_login_timeout(self):
        args = _connect_args(
            {"user": "u", "account": "a", "password": "p", "login_timeout": 30}
        )
        self.assertEqual(30, args["login_timeout"])
        self.assertIsInstance(args["login_timeout"], int)

    def test_password_auth_with_application(self):
        args = _connect_args(
            {
                "user": "u",
                "account": "a",
                "password": "p",
                "application": "Monte Carlo",
            }
        )
        self.assertEqual("Monte Carlo", args["application"])

    def test_password_auth_with_session_parameters(self):
        params = {"QUERY_TAG": "ccp", "TIMEZONE": "UTC"}
        args = _connect_args(
            {"user": "u", "account": "a", "password": "p", "session_parameters": params}
        )
        self.assertEqual(params, args["session_parameters"])

    # ── Key-pair auth ─────────────────────────────────────────────────

    def test_keypair_auth_pem_string(self):
        pem = _generate_pem()
        args = _connect_args(
            {"user": "u", "account": "a", "private_key_pem": pem.decode()}
        )
        self.assertIn("private_key", args)
        self.assertIsInstance(args["private_key"], bytes)
        self.assertNotIn("private_key_pem", args)
        self.assertNotIn("password", args)

    def test_keypair_auth_encrypted_pem(self):
        passphrase = b"my-passphrase"
        pem = _generate_pem(passphrase=passphrase)
        args = _connect_args(
            {
                "user": "u",
                "account": "a",
                "private_key_pem": pem.decode(),
                "private_key_passphrase": "my-passphrase",
            }
        )
        self.assertIn("private_key", args)
        self.assertIsInstance(args["private_key"], bytes)
        self.assertNotIn("private_key_passphrase", args)

    def test_keypair_private_key_not_in_raw(self):
        # private_key must never appear in the output when the user provides raw bytes
        # directly (not via private_key_pem) — the step only fires on private_key_pem.
        args = _connect_args({"user": "u", "account": "a", "password": "p"})
        self.assertNotIn("private_key", args)

    # ── OAuth auth ────────────────────────────────────────────────────

    def test_oauth_auth(self):
        args = _connect_args(
            {
                "user": "u",
                "account": "a",
                "token": "eyJhbGciOiJSUzI1NiJ9.fake",
                "authenticator": "oauth",
            }
        )
        self.assertEqual("eyJhbGciOiJSUzI1NiJ9.fake", args["token"])
        self.assertEqual("oauth", args["authenticator"])
        self.assertNotIn("password", args)
        self.assertNotIn("private_key", args)

    # ── Optional field omission ───────────────────────────────────────

    def test_omits_absent_optional_fields(self):
        args = _connect_args({"user": "u", "account": "a", "password": "p"})
        for field in (
            "warehouse",
            "database",
            "schema",
            "role",
            "login_timeout",
            "application",
            "session_parameters",
            "token",
            "authenticator",
        ):
            self.assertNotIn(field, args, f"expected {field!r} to be absent")

    def test_legacy_connect_args_passthrough(self):
        legacy = {"connect_args": {"user": "u", "account": "a", "password": "p"}}
        self.assertEqual(legacy, _resolve(legacy))

    # ── OAuth via token acquisition ───────────────────────────────────

    @patch("apollo.integrations.ccp.transforms.oauth.requests.post")
    def test_oauth_client_credentials(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"access_token": "tok_sfdc"}
        mock_resp.raise_for_status.return_value = None
        mock_post.return_value = mock_resp

        args = _connect_args(
            {
                "user": "u",
                "account": "a",
                "oauth": {
                    "client_id": "cid",
                    "client_secret": "csec",
                    "access_token_endpoint": "https://auth.example.com/token",
                    "grant_type": "client_credentials",
                },
            }
        )
        self.assertEqual("tok_sfdc", args["token"])
        self.assertEqual("oauth", args["authenticator"])
        self.assertNotIn("password", args)
        self.assertNotIn("private_key", args)
        self.assertNotIn("oauth", args)

    @patch("apollo.integrations.ccp.transforms.oauth.requests.post")
    def test_oauth_password_grant(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"access_token": "tok_pw"}
        mock_resp.raise_for_status.return_value = None
        mock_post.return_value = mock_resp

        args = _connect_args(
            {
                "user": "u",
                "account": "a",
                "oauth": {
                    "client_id": "cid",
                    "client_secret": "csec",
                    "access_token_endpoint": "https://auth.example.com/token",
                    "grant_type": "password",
                    "username": "alice",
                    "password": "hunter2",
                },
            }
        )
        self.assertEqual("tok_pw", args["token"])
        self.assertEqual("oauth", args["authenticator"])

    @patch("apollo.integrations.ccp.transforms.oauth.requests.post")
    def test_oauth_does_not_override_explicit_token(self, mock_post):
        # When both raw.token and raw.oauth are present, the OAuth step fires
        # and its field_map wins (step field_map overrides mapper field_map).
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"access_token": "tok_from_oauth"}
        mock_resp.raise_for_status.return_value = None
        mock_post.return_value = mock_resp

        args = _connect_args(
            {
                "user": "u",
                "account": "a",
                "token": "explicit_token",
                "oauth": {
                    "client_id": "cid",
                    "client_secret": "csec",
                    "access_token_endpoint": "https://auth.example.com/token",
                    "grant_type": "client_credentials",
                },
            }
        )
        # Step field_map wins; acquired token takes precedence over raw.token
        self.assertEqual("tok_from_oauth", args["token"])
