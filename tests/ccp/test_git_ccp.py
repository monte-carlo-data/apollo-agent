# tests/ccp/test_git_ccp.py
#
# GitProxyClient currently reads credentials flat, so GIT_DEFAULT_CCP is not
# registered in CcpRegistry._discover(). Tests import the config directly and
# call CcpPipeline().execute() rather than going through CcpRegistry.resolve().
from unittest import TestCase

from apollo.integrations.ccp.defaults.git import GIT_DEFAULT_CCP
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


def _resolve(credentials: dict) -> dict:
    return CcpPipeline().execute(GIT_DEFAULT_CCP, credentials)


class TestGitCcp(TestCase):
    def test_git_not_registered(self):
        self.assertIsNone(CcpRegistry.get("git"))

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
