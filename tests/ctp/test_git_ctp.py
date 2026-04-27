# tests/ctp/test_git_ctp.py
from unittest import TestCase

from apollo.integrations.ctp.defaults.git import GIT_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(GIT_DEFAULT_CTP, credentials)


class TestGitCtp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CtpRegistry.get("git"))

    def test_resolve_https_token_auth(self):
        result = _resolve(
            {
                "repo_url": "github.com/org/repo.git",
                "token": "ghp_abc123",
            }
        )
        self.assertEqual("github.com/org/repo.git", result["repo_url"])
        self.assertEqual("ghp_abc123", result["token"])
        self.assertNotIn("ssh_key", result)

    def test_resolve_https_with_username(self):
        result = _resolve(
            {
                "repo_url": "bitbucket.org/org/repo.git",
                "token": "mytoken",
                "username": "x-token-auth",
            }
        )
        self.assertEqual("x-token-auth", result["username"])

    def test_resolve_ssh_auth(self):
        result = _resolve(
            {
                "repo_url": "git@github.com:org/repo.git",
                "ssh_key": "base64encodedkey==",
            }
        )
        self.assertEqual("git@github.com:org/repo.git", result["repo_url"])
        self.assertEqual("base64encodedkey==", result["ssh_key"])
        self.assertNotIn("token", result)

    def test_omits_absent_optional_fields(self):
        result = _resolve({"repo_url": "github.com/org/repo.git"})
        self.assertNotIn("token", result)
        self.assertNotIn("username", result)
        self.assertNotIn("ssh_key", result)
        self.assertNotIn("ssl_options", result)

    def test_resolve_ssl_options_ca_data(self):
        """Regression: GitClientArgs mapper must include ssl_options or GitCloneClient
        never sees the CA bundle when the CTP path is active."""
        ca_pem = "-----BEGIN CERTIFICATE-----\nFAKECERT\n-----END CERTIFICATE-----\n"
        result = _resolve(
            {
                "repo_url": "self-managed.example.com/grp/repo.git",
                "token": "T",
                "ssl_options": {"ca_data": ca_pem},
            }
        )
        self.assertEqual({"ca_data": ca_pem}, result["ssl_options"])

    def test_resolve_ssl_options_skip_verification(self):
        result = _resolve(
            {
                "repo_url": "self-managed.example.com/grp/repo.git",
                "token": "T",
                "ssl_options": {"skip_verification": True},
            }
        )
        self.assertEqual({"skip_verification": True}, result["ssl_options"])

    def test_resolve_ssl_options_round_trips_extra_keys(self):
        """The default mapper passes the ssl_options dict through untouched —
        GitCloneClient is responsible for ignoring fields it does not consume."""
        result = _resolve(
            {
                "repo_url": "self-managed.example.com/grp/repo.git",
                "token": "T",
                "ssl_options": {
                    "ca_data": "PEM",
                    "cert_data": "X",
                    "mechanism": "url",
                },
            }
        )
        self.assertEqual(
            {"ca_data": "PEM", "cert_data": "X", "mechanism": "url"},
            result["ssl_options"],
        )
