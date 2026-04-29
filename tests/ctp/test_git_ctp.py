# tests/ctp/test_git_ctp.py
import os
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
        self.assertNotIn("ssl_ca_path", result)
        self.assertNotIn("ssl_skip_verification", result)

    def test_ssl_options_ca_data_resolves_to_ca_path(self):
        """Regression: resolve_ssl_options transform writes ca_data to a deterministic
        temp file and the mapper exposes it as connect_args.ssl_ca_path."""
        ca_pem = "-----BEGIN CERTIFICATE-----\nFAKECERT\n-----END CERTIFICATE-----\n"
        result = _resolve(
            {
                "repo_url": "self-managed.example.com/grp/repo.git",
                "token": "T",
                "ssl_options": {"ca_data": ca_pem},
            }
        )
        ca_path = result["ssl_ca_path"]
        self.assertTrue(ca_path.startswith("/tmp/"))
        self.assertTrue(ca_path.endswith("_ssl_ca.pem"))
        # Transform actually wrote the file with the supplied PEM.
        self.assertTrue(os.path.exists(ca_path))
        self.assertEqual(ca_pem, open(ca_path).read())
        self.assertNotIn("ssl_skip_verification", result)
        # Cleanup.
        os.remove(ca_path)

    def test_ssl_options_skip_cert_verification_resolves(self):
        result = _resolve(
            {
                "repo_url": "self-managed.example.com/grp/repo.git",
                "token": "T",
                "ssl_options": {"skip_cert_verification": True},
            }
        )
        self.assertTrue(result["ssl_skip_verification"])
        self.assertNotIn("ssl_ca_path", result)

    def test_ssl_options_disabled_omits_ca_path(self):
        """ssl_options.disabled=True suppresses the CA file write even if ca_data is present."""
        # SslOptions.__post_init__ raises if disabled is set with ca_data, so use a clean disable.
        result = _resolve(
            {
                "repo_url": "self-managed.example.com/grp/repo.git",
                "token": "T",
                "ssl_options": {"disabled": True},
            }
        )
        self.assertNotIn("ssl_ca_path", result)
