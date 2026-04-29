import base64
from unittest import TestCase
from unittest.mock import patch, create_autospec, ANY

from apollo.agent.utils import AgentUtils
from apollo.integrations.git.git_client import GitCloneClient, GitFileData
from apollo.integrations.git.git_proxy_client import GitProxyClient
from apollo.integrations.storage.storage_proxy_client import StorageProxyClient


class GitTests(TestCase):
    @patch("apollo.integrations.git.git_client.git")
    @patch("apollo.integrations.git.git_proxy_client.StorageProxyClient")
    @patch.object(GitCloneClient, "read_files")
    @patch("apollo.integrations.git.git_proxy_client.zipfile.ZipFile")
    @patch("apollo.integrations.git.git_proxy_client.os.remove")
    @patch.object(AgentUtils, "temp_file_path")
    def test_ssh_clone(
        self,
        utils_tmp,
        remove_mock,
        zip_file_mock,
        read_files_mock,
        storage_mock,
        git_mock,
    ):
        client = GitProxyClient(
            credentials={
                "repo_url": "git@github.com:gh_account/repo.git",
                "ssh_key": base64.b64encode("SSH TEST KEY".encode("utf-8")),
            },
            platform="GCP",
        )
        storage_client = create_autospec(StorageProxyClient)
        storage_mock.return_value = storage_client
        storage_client.generate_presigned_url.return_value = "https://pre-signed-url"
        read_files_mock.return_value = [
            GitFileData("a.txt", "aaaa"),
        ]
        zip_file = "/tmp/test.zip"
        utils_tmp.return_value = zip_file

        result = client.get_files(["txt"])

        git_mock.exec_command.assert_called_with(
            "clone",
            "--depth",
            "1",
            "git@github.com:gh_account/repo.git",
            "/tmp/repo",
            env=ANY,
        )

        storage_client.upload_file.assert_called_with(ANY, zip_file)
        storage_client.generate_presigned_url.assert_called()
        remove_mock.assert_called_with(zip_file)

        self.assertEqual("https://pre-signed-url", result["url"])
        self.assertIsNotNone(result.get("key"))

    @patch("apollo.integrations.git.git_client.git")
    def test_https_clone_without_ssl_args(self, git_mock):
        """No SSL args => existing git clone command, no -c flags."""
        client = GitCloneClient(
            credentials={
                "repo_url": "github.com/gh_account/repo.git",
                "token": "TOKEN",
            }
        )
        client._https_git_clone()
        git_mock.exec_command.assert_called_once_with(
            "clone",
            "--depth",
            "1",
            "https://oauth2:TOKEN@github.com/gh_account/repo.git",
            "/tmp/repo",
        )

    @patch("apollo.integrations.git.git_client.git")
    def test_https_clone_with_ssl_ca_path(self, git_mock):
        """ssl_ca_path (resolved by the resolve_ssl_options CTP transform) =>
        passes -c http.sslCAInfo=<path>."""
        client = GitCloneClient(
            credentials={
                "repo_url": "self-managed.example.com/grp/repo.git",
                "token": "TOKEN",
                "ssl_ca_path": "/tmp/abc123_ssl_ca.pem",
            }
        )
        client._https_git_clone()
        git_mock.exec_command.assert_called_once_with(
            "-c",
            "http.sslCAInfo=/tmp/abc123_ssl_ca.pem",
            "clone",
            "--depth",
            "1",
            "https://oauth2:TOKEN@self-managed.example.com/grp/repo.git",
            "/tmp/repo",
        )

    @patch("apollo.integrations.git.git_client.git")
    def test_https_clone_with_ssl_skip_verification(self, git_mock):
        """ssl_skip_verification=True => -c http.sslVerify=false."""
        client = GitCloneClient(
            credentials={
                "repo_url": "github.com/gh_account/repo.git",
                "token": "TOKEN",
                "ssl_skip_verification": True,
            }
        )
        client._https_git_clone()
        git_mock.exec_command.assert_called_once_with(
            "-c",
            "http.sslVerify=false",
            "clone",
            "--depth",
            "1",
            "https://oauth2:TOKEN@github.com/gh_account/repo.git",
            "/tmp/repo",
        )

    @patch("apollo.integrations.git.git_client.git")
    def test_https_clone_with_ssl_ca_path_and_skip_verification(self, git_mock):
        client = GitCloneClient(
            credentials={
                "repo_url": "self-managed.example.com/grp/repo.git",
                "token": "TOKEN",
                "ssl_ca_path": "/tmp/abc123_ssl_ca.pem",
                "ssl_skip_verification": True,
            }
        )
        client._https_git_clone()
        git_mock.exec_command.assert_called_once_with(
            "-c",
            "http.sslCAInfo=/tmp/abc123_ssl_ca.pem",
            "-c",
            "http.sslVerify=false",
            "clone",
            "--depth",
            "1",
            "https://oauth2:TOKEN@self-managed.example.com/grp/repo.git",
            "/tmp/repo",
        )

    @patch("apollo.integrations.git.git_client.git")
    def test_https_clone_via_connect_args_nesting(self, git_mock):
        """The CTP path nests resolved fields under connect_args; client must read both shapes."""
        client = GitCloneClient(
            credentials={
                "connect_args": {
                    "repo_url": "self-managed.example.com/grp/repo.git",
                    "token": "TOKEN",
                    "ssl_ca_path": "/tmp/abc123_ssl_ca.pem",
                }
            }
        )
        client._https_git_clone()
        git_mock.exec_command.assert_called_once_with(
            "-c",
            "http.sslCAInfo=/tmp/abc123_ssl_ca.pem",
            "clone",
            "--depth",
            "1",
            "https://oauth2:TOKEN@self-managed.example.com/grp/repo.git",
            "/tmp/repo",
        )

    @patch("apollo.integrations.git.git_client.git")
    def test_ssh_clone_ignores_ssl_args(self, git_mock):
        """SSL args are irrelevant for SSH clones; never produce -c flags."""
        client = GitCloneClient(
            credentials={
                "repo_url": "git@github.com:gh_account/repo.git",
                "ssh_key": base64.b64encode(b"SSH TEST KEY"),
                "ssl_ca_path": "/tmp/abc123_ssl_ca.pem",
                "ssl_skip_verification": True,
            }
        )
        with patch.object(client, "_KEY_FILE"):
            client.write_key()
            client._ssh_git_clone()
        git_mock.exec_command.assert_called_with(
            "clone",
            "--depth",
            "1",
            "git@github.com:gh_account/repo.git",
            "/tmp/repo",
            env=ANY,
        )

    @patch("apollo.integrations.git.git_client.git")
    def test_git_version(self, git_mock):
        client = GitProxyClient(
            credentials={
                "repo_url": "git@github.com:gh_account/repo.git",
                "ssh_key": base64.b64encode("SSH TEST KEY".encode("utf-8")),
            },
            platform="GCP",
        )
        git_mock.exec_command.return_value = ("1.0.1".encode("utf-8"), None)
        result = client.wrapped_client.git_version()

        git_mock.exec_command.assert_called_with("--version")

        self.assertEqual("1.0.1", result["stdout"])
        self.assertEqual("", result["stderr"])
