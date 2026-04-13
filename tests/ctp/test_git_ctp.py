# tests/ctp/test_git_ctp.py
#
# GitProxyClient currently reads credentials flat, so GIT_DEFAULT_CTP is not
# registered in CtpRegistry._discover(). Tests import the config directly and
# call CtpPipeline().execute() rather than going through CtpRegistry.resolve().
from unittest import TestCase

from apollo.integrations.ctp.defaults.git import GIT_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(GIT_DEFAULT_CTP, credentials)


class TestGitCtp(TestCase):
    def test_git_not_registered(self):
        self.assertIsNone(CtpRegistry.get("git"))

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
