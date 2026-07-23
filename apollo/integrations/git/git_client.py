import base64
import logging
import os
import re
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Dict, List, Generator

import git

logger = logging.getLogger(__name__)


@dataclass
class GitFileData:
    name: str
    content: str


class GitCloneClient:
    """
    Git client used to clone a repo, both ssh and https protocols are supported.
    It exposes a method that returns an iterator for all files matching a list of extensions.

    SSL options for HTTPS clones (no-op for SSH) are resolved upstream by the
    ``resolve_ssl_options`` CTP transform and arrive on connect_args as:

    - ``ssl_ca_path`` (str): path to a PEM CA bundle written by the transform.
      Passed to git via ``-c http.sslCAInfo=<path>``. Use this for self-managed
      Git providers whose root CA is not in the agent image's default trust store.
    - ``ssl_skip_verification`` (bool): when truthy, sets ``-c http.sslVerify=false``
      and logs a warning.
    """

    GIT_TYPE = {"ssh": "SSH", "https": "HTTPS"}

    def __init__(self, credentials: Dict, **kwargs):  # type: ignore
        creds = credentials.get("connect_args", credentials)
        self._repo_url = creds["repo_url"]
        self._token = creds.get("token")
        self._username = creds.get("username")
        self._ssh_key = base64.b64decode(creds.get("ssh_key", ""))
        self._ssl_ca_path: str | None = creds.get("ssl_ca_path")
        self._ssl_skip_verification = bool(creds.get("ssl_skip_verification"))

        # Unique per-client paths (only /tmp is writable in lambda). A fixed path
        # would collide when git operations run concurrently — possible on
        # long-lived runtimes (EKS/hermes, Azure) even if not on Lambda/Cloud Run —
        # letting one request overwrite/delete another's repo or SSH key mid-clone.
        # The repo dir name is unguessable and created by `git clone`; the SSH key
        # file is created per-write via mkstemp (see write_key).
        self._repo_dir = Path(tempfile.gettempdir()) / f"mcd_repo_{uuid.uuid4().hex}"
        self._key_file: Path | None = None

        if self._token:
            # remove the scheme if it was included so the token can be inserted before the host
            self._repo_url = self._repo_url.removeprefix("https://")

    def get_files(
        self, file_extensions: List[str]
    ) -> Generator[GitFileData, None, None]:
        """
        Main method, reads the content of all files filtering by the specified extensions.
        :param file_extensions: list of extensions to filter the returned files by, for example "lkml".
        :return: a generator that returns GitFileData objects for each file with on of the specified extensions.
        """
        try:
            if self._ssh_key:
                self.write_key()
            self.delete_repo_dir()  # Prepare
            self.git_clone()
            yield from self.read_files(file_extensions)
        finally:
            # Always clean up, even on error: the repo dir and especially the
            # SSH private key must not persist for the lifetime of the container.
            self.delete_repo_dir()
            self.delete_key()

    def delete_repo_dir(self):
        """Delete the repo directory if it exists."""
        if self._repo_dir.exists():
            logger.info(f"Delete repo dir: {self._repo_dir}")
            shutil.rmtree(self._repo_dir)

    def delete_key(self):
        """Delete the SSH private key file if it exists."""
        if self._key_file and self._key_file.exists():
            logger.info(f"Delete key file: {self._key_file}")
            self._key_file.unlink()

    @staticmethod
    def git_version() -> Dict:
        """
        Executes `git --version` and returns the output in a dictionary with two keys: `stdout` and `stderr`.
        :return: the output for `git --version`.
        """
        stdout, stderr = git.exec_command("--version")  # type: ignore[attr-defined]
        return {
            "stdout": stdout.decode("utf-8") if stdout else "",
            "stderr": stderr.decode("utf-8") if stderr else "",
        }

    def git_clone(self):
        """
        Clones a git repo.

        Will use ssh if an ssh key is provided and will use https if it is not present.
        """
        if self._ssh_key:
            self._ssh_git_clone()
        else:
            self._https_git_clone()

    def read_files(
        self, file_extensions: List[str]
    ) -> Generator[GitFileData, None, None]:
        """
        Traverse a directory, selecting only files with the given extensions. It is important for this to return
        a generator to avoid loading the content of all files into memory.
        """
        logger.info(f'Read files with extensions: {",".join(file_extensions)}')
        globs = [self._repo_dir.rglob(f"*.{ext}") for ext in file_extensions]
        for file in chain(*globs):  # globs are generators, need to be chained.
            if file.is_file():
                original_name = str(
                    file.relative_to(self._repo_dir)
                )  # Drop local dir from file name.
                yield GitFileData(
                    original_name, file.read_text(errors="replace")
                )  # Replace encoding errors with "?".

    def _https_git_clone(self):
        """
        Clone a git repo. It uses Https and a git authorization token.

        "repo_url" can be a full git https URL (https://server/project.git) or the shorter version
        (server/project.git).
        """
        logger.info(f"Clone repo: {self._repo_url}")
        url = f"https://oauth2:{self._token}@{self._repo_url}"
        if self._username:
            # This allows for support of bitbucket as they handle access tokens slightly differently
            url = f"https://{self._username}:{self._token}@{self._repo_url}"
        # Use depth 1 to bring only the latest revision.
        ssl_config = self._build_ssl_config_args()
        git_params = [*ssl_config, "clone", "--depth", "1", url, str(self._repo_dir)]
        try:
            git.exec_command(*git_params)  # type: ignore[attr-defined]
        except git.exceptions.GitExecutionError as e:  # type: ignore[attr-defined]
            password_removed_message = self._replace_passwords_in_urls(str(e))

            raise git.exceptions.GitExecutionError(password_removed_message)  # type: ignore[attr-defined]

    def _build_ssl_config_args(self) -> List[str]:
        """
        Translate the resolved SSL connect args (from the ``resolve_ssl_options`` CTP
        transform) into ``git -c <key>=<value>`` arguments for HTTPS clones.
        """
        args: List[str] = []
        if self._ssl_ca_path:
            args.extend(["-c", f"http.sslCAInfo={self._ssl_ca_path}"])
        if self._ssl_skip_verification:
            logger.warning(
                "TLS verification disabled for git clone via ssl_options.skip_cert_verification"
            )
            args.extend(["-c", "http.sslVerify=false"])
        return args

    @staticmethod
    def _replace_passwords_in_urls(text: str, placeholder: str = "********"):
        pattern = r"(?<=://)(.*?:)(.*?)(?=@)"
        replaced_text = re.sub(pattern, r"\1" + placeholder, text)
        return replaced_text

    def write_key(self):
        """Write the SSH key to a fresh, uniquely-named 0o600 temp file.

        ``mkstemp`` creates the file owner-only and with an unguessable name, so
        it won't follow a pre-planted symlink and won't collide with a concurrent
        clone's key. The path is stored on the instance for ``_ssh_git_clone`` and
        ``delete_key``.
        """
        if self._ssh_key:
            fd, path = tempfile.mkstemp(prefix="mcd_rsa_")
            os.close(fd)
            self._key_file = Path(path)
            logger.info(f"Write key to file: {self._key_file}")
            self._key_file.write_bytes(self._ssh_key)
            self._key_file.chmod(0o400)

    def _ssh_git_clone(self):
        """
        Clone a git repo. It uses GIT_SSH_COMMAND to pass the SSH key and to set SSH options necessary to run git in
        the Lambda environment.

        "repo_url" can be a full ssh URL (ssh://[user@]server/project.git) or the shorter version
        ([user@]server:project.git).
        """
        logger.info(f"Clone repo: {self._repo_url}")
        ssh_options = " ".join(
            [
                "-v",  # Verbose helps in case of problems
                "-o StrictHostKeyChecking=no",  # We do not know all the possible hosts, so do not check
                "-o UserKnownHostsFile=/dev/null",  # Do not write known_hosts
                "-o GlobalKnownHostsFile=/dev/null",  # Do not write known_hosts
                f"-i {self._key_file}",  # Use this key
            ]
        )
        env = {**os.environ, **{"GIT_SSH_COMMAND": f"ssh {ssh_options}"}}
        # Use depth 1 to bring only the latest revision.
        git_params = ["clone", "--depth", "1", self._repo_url, str(self._repo_dir)]
        git.exec_command(*git_params, env=env)  # type: ignore[attr-defined]

    @property
    def repo_url(self):
        """
        Returns the url used to configure this client in `credentials["repo_url"]`.
        """
        return self._repo_url
