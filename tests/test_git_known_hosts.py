"""
SEC-4040: Git-over-SSH clones verify the server's host key.

Unit tests check the ssh options and the per-clone known_hosts file. The tests in
``RealSshHostKeyTests`` run the real ``ssh``/``git`` binaries against the public
Git hosts (they run in the Docker ``tests`` stage, so they exercise the image's
OpenSSH); they are skipped when the host is unreachable.
"""

import base64
import hashlib
import json
import logging
import os
import shutil
import socket
import subprocess
import tempfile
import urllib.error
import urllib.request
from contextlib import ExitStack
from pathlib import Path
from unittest import TestCase, skipUnless
from unittest.mock import ANY, patch

import git

from apollo.integrations.git import git_client
from apollo.integrations.git.git_client import (
    BUNDLED_KNOWN_HOSTS_PATH,
    GitCloneClient,
    GitFileData,
)

# Published SHA256 fingerprints. GitHub: api.github.com/meta ssh_key_fingerprints.
# GitLab: the GitLab.com docs ("SSH host keys fingerprints"). Bitbucket: computed
# from bitbucket.org/site/ssh. A change to the bundled file must match these.
PUBLISHED_FINGERPRINTS = {
    "github.com": {
        "SHA256:+DiY3wvvV6TuJJhbpZisF/zLDA0zPMSvHdkr4UvCOqU",
        "SHA256:p2QAMXNIC1TJYWeIOttrVc98/R1BUFWu3/LiyKgUfQM",
        "SHA256:uNiVztksCsDhcc0u9e8BujQXVUpKZIDTMczCvj3tD2s",
    },
    "gitlab.com": {
        "SHA256:eUXGGm1YGsMAS7vkcx6JOJdOGHPem5gQp4taiCfCLB8",
        "SHA256:HbW3g8zUjNSksFbqTiUWPWg2Bq1x8xdGUrliXFzSnUw",
        "SHA256:ROQFvPThGrW4RuWLoL9tq9I9zJ42fK4XywyRtbOz/EQ",
    },
    "bitbucket.org": {
        "SHA256:ybgmFkzwOSotHTHLJgHO0QN8L0xErw6vd0VhFA9m3SM",
        "SHA256:FC73VB6C4OQLSCrjEayhMp9UMxS97caD/Yyi2bhW/J0",
        "SHA256:46OSHA1Rmj8E8ERTC6xkNcmGOw9oFxYr0WF6zWW8l1E",
    },
}
PUBLISHED_FINGERPRINTS["[ssh.github.com]:443"] = PUBLISHED_FINGERPRINTS["github.com"]
PUBLISHED_FINGERPRINTS["ssh.bitbucket.org"] = PUBLISHED_FINGERPRINTS["bitbucket.org"]

SSH_KEY = base64.b64encode(b"SSH TEST KEY")
SELF_HOSTED_LINE = (
    "git.example.com ssh-ed25519 "
    "AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"
)


def _bundled_entries() -> list[tuple[str, str, str]]:
    entries = []
    for line in BUNDLED_KNOWN_HOSTS_PATH.read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            hosts, key_type, key = line.split()
            for host in hosts.split(","):
                entries.append((host, key_type, key))
    return entries


def _fingerprint(key_b64: str) -> str:
    digest = hashlib.sha256(base64.b64decode(key_b64)).digest()
    return "SHA256:" + base64.b64encode(digest).decode().rstrip("=")


def _option(ssh_command: str, name: str) -> str:
    prefix = f"{name}="
    for part in ssh_command.split():
        if part.startswith(prefix):
            return part[len(prefix) :]
    raise AssertionError(f"{name} not set in {ssh_command!r}")


class BundledKnownHostsTests(TestCase):
    def test_bundled_keys_match_published_fingerprints(self):
        by_host: dict[str, set[str]] = {}
        for host, _type, key in _bundled_entries():
            by_host.setdefault(host, set()).add(_fingerprint(key))
        self.assertEqual(PUBLISHED_FINGERPRINTS, by_host)

    def test_pinned_host_names(self):
        self.assertEqual(
            set(PUBLISHED_FINGERPRINTS), GitCloneClient.pinned_host_names()
        )


class SshKnownHostsNameTests(TestCase):
    def test_parses_ssh_urls(self):
        cases = {
            "git@github.com:org/repo.git": "github.com",
            "GIT@GitHub.com:org/repo.git": "github.com",
            "github.com:org/repo.git": "github.com",
            "ssh://git@github.com/org/repo.git": "github.com",
            "ssh://git@github.com:22/org/repo.git": "github.com",
            "ssh://git@ssh.github.com:443/org/repo.git": "[ssh.github.com]:443",
            "ssh://git@git.example.com:2222/grp/repo.git": "[git.example.com]:2222",
            "ssh://git@[2001:db8::1]:2222/repo.git": "[2001:db8::1]:2222",
            "git@[git.example.com]:repo.git": "git.example.com",
            "org-1234@bitbucket.org:org/repo.git": "bitbucket.org",
        }
        for url, expected in cases.items():
            with self.subTest(url=url):
                self.assertEqual(expected, GitCloneClient.ssh_known_hosts_name(url))

    def test_unparseable_url(self):
        self.assertIsNone(GitCloneClient.ssh_known_hosts_name("/local/path/repo"))


class SshCommandTests(TestCase):
    def _client(self, repo_url: str, **creds) -> GitCloneClient:
        client = GitCloneClient(
            credentials={"repo_url": repo_url, "ssh_key": SSH_KEY, **creds}
        )
        client.write_key()
        self.addCleanup(client.delete_key)
        self.addCleanup(client.delete_known_hosts)
        return client

    def test_pinned_hosts_are_strict(self):
        for url in [
            "git@github.com:org/repo.git",
            "ssh://git@ssh.github.com:443/org/repo.git",
            "git@gitlab.com:grp/repo.git",
            "git@bitbucket.org:ws/repo.git",
        ]:
            with self.subTest(url=url):
                command = self._client(url).ssh_command()
                self.assertEqual("yes", _option(command, "StrictHostKeyChecking"))
                self.assertEqual("/dev/null", _option(command, "GlobalKnownHostsFile"))
                self.assertNotIn("StrictHostKeyChecking=no", command)
                self.assertNotIn("UserKnownHostsFile=/dev/null", command)

    def test_known_hosts_file_holds_pinned_keys(self):
        client = self._client("git@github.com:org/repo.git")
        known_hosts = Path(_option(client.ssh_command(), "UserKnownHostsFile"))
        self.assertEqual(client._known_hosts_file, known_hosts)
        self.assertEqual(BUNDLED_KNOWN_HOSTS_PATH.read_text(), known_hosts.read_text())
        self.assertEqual(0o600, known_hosts.stat().st_mode & 0o777)

    def test_connection_known_hosts_makes_self_hosted_strict(self):
        client = self._client(
            "git@git.example.com:grp/repo.git", ssh_known_hosts=SELF_HOSTED_LINE
        )
        command = client.ssh_command()
        self.assertEqual("yes", _option(command, "StrictHostKeyChecking"))
        content = Path(_option(command, "UserKnownHostsFile")).read_text()
        self.assertIn(SELF_HOSTED_LINE, content.splitlines())
        self.assertIn("github.com,[ssh.github.com]:443 ssh-ed25519", content)

    def test_connect_args_nesting(self):
        client = GitCloneClient(
            credentials={
                "connect_args": {
                    "repo_url": "git@git.example.com:grp/repo.git",
                    "ssh_key": SSH_KEY,
                    "ssh_known_hosts": SELF_HOSTED_LINE,
                }
            }
        )
        self.assertEqual("yes", client.ssh_strict_host_key_checking())

    def test_self_hosted_without_known_hosts_warns(self):
        client = self._client("git@git.example.com:grp/repo.git")
        with self.assertLogs(git_client.logger, logging.WARNING) as logs:
            command = client.ssh_command()
        self.assertEqual("accept-new", _option(command, "StrictHostKeyChecking"))
        self.assertIn("git.example.com", "\n".join(logs.output))
        # accept-new still refuses a wrong key for the pinned hosts in the file.
        content = Path(_option(command, "UserKnownHostsFile")).read_text()
        self.assertIn("github.com,[ssh.github.com]:443 ssh-ed25519", content)

    @patch("apollo.integrations.git.git_client.git")
    def test_clone_passes_ssh_command(self, git_mock):
        client = self._client("git@github.com:org/repo.git")
        client._ssh_git_clone()
        git_mock.exec_command.assert_called_once_with(
            "clone",
            "--depth",
            "1",
            "git@github.com:org/repo.git",
            str(client._repo_dir),
            env=ANY,
        )
        command = git_mock.exec_command.call_args.kwargs["env"]["GIT_SSH_COMMAND"]
        self.assertEqual("yes", _option(command, "StrictHostKeyChecking"))
        self.assertIn(f"-i {client._key_file}", command)


class KnownHostsCleanupTests(TestCase):
    def _client(self) -> GitCloneClient:
        return GitCloneClient(
            credentials={"repo_url": "git@github.com:org/repo.git", "ssh_key": SSH_KEY}
        )

    @patch("apollo.integrations.git.git_client.git")
    @patch.object(GitCloneClient, "delete_repo_dir")
    def test_known_hosts_deleted_after_get_files(self, _delete_repo, _git):
        client = self._client()
        with patch.object(client, "read_files", return_value=[GitFileData("a", "x")]):
            list(client.get_files(["txt"]))
        self.assertIsNotNone(client._known_hosts_file)
        self.assertFalse(client._known_hosts_file.exists())

    @patch("apollo.integrations.git.git_client.git")
    @patch.object(GitCloneClient, "delete_repo_dir")
    def test_known_hosts_deleted_when_clone_fails(self, _delete_repo, git_mock):
        git_mock.exec_command.side_effect = RuntimeError(
            "Host key verification failed."
        )
        client = self._client()
        with self.assertRaises(RuntimeError):
            list(client.get_files(["txt"]))
        self.assertIsNotNone(client._known_hosts_file)
        self.assertFalse(client._known_hosts_file.exists())


def _reachable(host: str, port: int = 22) -> bool:
    try:
        with socket.create_connection((host, port), timeout=5):
            return True
    except OSError:
        return False


_NO_AGENT_ENV = {k: v for k, v in os.environ.items() if k != "SSH_AUTH_SOCK"}
HAVE_SSH = shutil.which("ssh") is not None and shutil.which("ssh-keygen") is not None
GITHUB_REACHABLE = HAVE_SSH and _reachable("github.com")


class RealSshHostKeyTests(TestCase):
    """
    Real OpenSSH against the live hosts, with a throwaway client key. GitHub, GitLab
    and Bitbucket reject the key ("Permission denied (publickey)"), which happens
    only after the host key was verified. A wrong pinned key must stop the
    connection at "Host key verification failed" instead.
    """

    HOST_KEY_FAILED = "Host key verification failed"
    AUTH_DENIED = "Permission denied (publickey"

    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls.key_path = Path(cls._tmp.name) / "id_ed25519"
        if HAVE_SSH:
            subprocess.run(
                [
                    "ssh-keygen",
                    "-q",
                    "-t",
                    "ed25519",
                    "-N",
                    "",
                    "-f",
                    str(cls.key_path),
                ],
                check=True,
            )

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def _wrong_known_hosts(self) -> Path:
        # github.com pinned to GitLab's ed25519 key: a stand-in for an impostor.
        gitlab_key = next(
            key
            for host, key_type, key in _bundled_entries()
            if host == "gitlab.com" and key_type == "ssh-ed25519"
        )
        path = Path(self._tmp.name) / "wrong_known_hosts"
        path.write_text(f"github.com ssh-ed25519 {gitlab_key}\n")
        return path

    def _ssh(self, host: str, known_hosts: Path, port: int = 22):
        return subprocess.run(
            [
                "ssh",
                "-F",
                "/dev/null",  # ignore the developer's ~/.ssh/config
                "-T",
                "-p",
                str(port),
                "-o",
                "StrictHostKeyChecking=yes",
                "-o",
                f"UserKnownHostsFile={known_hosts}",
                "-o",
                "GlobalKnownHostsFile=/dev/null",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=15",
                "-o",
                "IdentitiesOnly=yes",
                "-i",
                str(self.key_path),
                f"git@{host}",
            ],
            env=_NO_AGENT_ENV,
            capture_output=True,
            text=True,
            timeout=60,
        )

    def test_pinned_keys_verify_live_hosts(self):
        for host in ["github.com", "gitlab.com", "bitbucket.org"]:
            with self.subTest(host=host):
                if not (HAVE_SSH and _reachable(host)):
                    self.skipTest(f"ssh or {host}:22 not available")
                result = self._ssh(host, BUNDLED_KNOWN_HOSTS_PATH)
                self.assertNotIn(self.HOST_KEY_FAILED, result.stderr)
                self.assertIn(self.AUTH_DENIED, result.stderr)

    @skipUnless(GITHUB_REACHABLE, "ssh or github.com:22 not available")
    def test_wrong_pinned_key_fails(self):
        result = self._ssh("github.com", self._wrong_known_hosts())
        self.assertNotEqual(0, result.returncode)
        self.assertIn(self.HOST_KEY_FAILED, result.stderr)
        self.assertNotIn(self.AUTH_DENIED, result.stderr)

    def _clone(self, known_hosts: Path | None = None) -> str:
        client = GitCloneClient(
            credentials={
                "repo_url": "git@github.com:monte-carlo-data/apollo-agent.git",
                "ssh_key": base64.b64encode(self.key_path.read_bytes()),
            }
        )
        real_ssh_command = GitCloneClient.ssh_command

        def isolated_ssh_command(client_self):
            # Only add isolation from a developer's ssh config and agent; every
            # host key option comes from the client unchanged.
            command = real_ssh_command(client_self)
            return command.replace("ssh ", "ssh -F /dev/null -o IdentitiesOnly=yes ", 1)

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(GitCloneClient, "ssh_command", isolated_ssh_command)
            )
            stack.enter_context(patch.dict(os.environ, _NO_AGENT_ENV, clear=True))
            if known_hosts:
                stack.enter_context(
                    patch.object(git_client, "BUNDLED_KNOWN_HOSTS_PATH", known_hosts)
                )
            with self.assertRaises(git.exceptions.GitExecutionError) as ctx:  # type: ignore[attr-defined]
                list(client.get_files(["md"]))
        self.assertIsNotNone(client._known_hosts_file)
        self.assertFalse(client._known_hosts_file.exists())
        return str(ctx.exception)

    @skipUnless(GITHUB_REACHABLE, "ssh or github.com:22 not available")
    def test_clone_verifies_github_host_key(self):
        error = self._clone()
        self.assertNotIn(self.HOST_KEY_FAILED, error)
        self.assertIn(self.AUTH_DENIED, error)

    @skipUnless(GITHUB_REACHABLE, "ssh or github.com:22 not available")
    def test_clone_rejects_wrong_github_host_key(self):
        error = self._clone(self._wrong_known_hosts())
        self.assertIn(self.HOST_KEY_FAILED, error)
        self.assertNotIn(self.AUTH_DENIED, error)


class GitHubMetaTests(TestCase):
    """The pinned GitHub keys still match GitHub's published list (rotation check)."""

    def test_bundled_github_keys_match_api(self):
        try:
            with urllib.request.urlopen(
                "https://api.github.com/meta", timeout=15
            ) as resp:
                published = set(json.load(resp)["ssh_keys"])
        except (urllib.error.URLError, OSError, KeyError, ValueError) as e:
            self.skipTest(f"api.github.com/meta unavailable: {e}")
        bundled = {
            f"{key_type} {key}"
            for host, key_type, key in _bundled_entries()
            if host == "github.com"
        }
        self.assertEqual(published, bundled)
