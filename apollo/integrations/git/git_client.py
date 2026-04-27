import base64
import logging
import os
import re
import shutil
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Dict, List, Generator, Optional

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

    Optional ``ssl_options`` (dict, HTTPS only) honored on the credentials payload:

    - ``ca_data`` (str, PEM-encoded): inline CA bundle written to ``_CA_BUNDLE_FILE`` and
      passed to git via ``-c http.sslCAInfo``. Use this for self-managed Git providers
      whose root CA is not in the agent image's default trust store.
    - ``skip_verification`` / ``skip_cert_verification`` (bool): when truthy, sets
      ``-c http.sslVerify=false``. Logged as a warning.

    Unknown keys in ``ssl_options`` are ignored. ``ssl_options`` is no-op for SSH clones.
    """

    # Only /tmp is writable in lambda.
    _REPO_DIR = Path("/tmp/repo")
    _KEY_FILE = Path("/tmp/mcd_rsa")
    _CA_BUNDLE_FILE = Path("/tmp/git_ca_bundle.pem")

    GIT_TYPE = {"ssh": "SSH", "https": "HTTPS"}

    def __init__(self, credentials: Dict, **kwargs):  # type: ignore
        creds = credentials.get("connect_args", credentials)
        self._repo_url = creds["repo_url"]
        self._token = creds.get("token")
        self._username = creds.get("username")
        self._ssh_key = base64.b64decode(creds.get("ssh_key", ""))
        self._ssl_options: Dict = creds.get("ssl_options") or {}

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
        if self._ssh_key:
            self.write_key()
        self.delete_repo_dir()  # Prepare
        self.git_clone()
        yield from self.read_files(file_extensions)
        self.delete_repo_dir()  # Clean up

    def delete_repo_dir(self):
        """Delete a directory if it exists."""
        if self._REPO_DIR.exists():
            logger.info(f"Delete repo dir: {self._REPO_DIR}")
            shutil.rmtree(self._REPO_DIR)

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
        globs = [self._REPO_DIR.rglob(f"*.{ext}") for ext in file_extensions]
        for file in chain(*globs):  # globs are generators, need to be chained.
            if file.is_file():
                original_name = str(
                    file.relative_to(self._REPO_DIR)
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
        git_params = [*ssl_config, "clone", "--depth", "1", url, str(self._REPO_DIR)]
        try:
            git.exec_command(*git_params)  # type: ignore[attr-defined]
        except git.exceptions.GitExecutionError as e:  # type: ignore[attr-defined]
            password_removed_message = self._replace_passwords_in_urls(str(e))

            raise git.exceptions.GitExecutionError(password_removed_message)  # type: ignore[attr-defined]

    def _build_ssl_config_args(self) -> List[str]:
        """
        Translate ``ssl_options`` into ``git -c <key>=<value>`` arguments for HTTPS clones.

        Returns an empty list when no SSL options are supplied or when only unsupported
        keys are present. Writes the inline CA bundle to ``_CA_BUNDLE_FILE`` when ``ca_data``
        is set so git can read it via ``http.sslCAInfo``.
        """
        if not self._ssl_options:
            return []

        args: List[str] = []
        ca_data: Optional[str] = self._ssl_options.get("ca_data")
        # Accept both monolith v1 (skip_verification) and v2 (skip_cert_verification) field names.
        skip_verification = bool(
            self._ssl_options.get("skip_verification")
            or self._ssl_options.get("skip_cert_verification")
        )

        if ca_data:
            self._write_ca_bundle(ca_data)
            args.extend(["-c", f"http.sslCAInfo={self._CA_BUNDLE_FILE}"])

        if skip_verification:
            logger.warning(
                "TLS verification disabled for git clone via ssl_options.skip_verification"
            )
            args.extend(["-c", "http.sslVerify=false"])

        return args

    def _write_ca_bundle(self, ca_data: str) -> None:
        """Persist a PEM-encoded CA bundle to a writable location for git's sslCAInfo."""
        logger.info(f"Write CA bundle to file: {self._CA_BUNDLE_FILE}")
        self._CA_BUNDLE_FILE.write_text(ca_data)
        self._CA_BUNDLE_FILE.chmod(0o400)

    @staticmethod
    def _replace_passwords_in_urls(text: str, placeholder: str = "********"):
        pattern = r"(?<=://)(.*?:)(.*?)(?=@)"
        replaced_text = re.sub(pattern, r"\1" + placeholder, text)
        return replaced_text

    def write_key(self):
        """Write SSH key to a file, overwriting it if file exists."""
        if self._ssh_key:
            if self._KEY_FILE.exists():
                logger.info(f"Key already exists: {self._KEY_FILE}, overwrite it")
                self._KEY_FILE.chmod(0o600)
            logger.info(f"Write key to file: {self._KEY_FILE}")
            self._KEY_FILE.write_bytes(self._ssh_key)
            self._KEY_FILE.chmod(0o400)

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
                f"-i {self._KEY_FILE}",  # Use this key
            ]
        )
        env = {**os.environ, **{"GIT_SSH_COMMAND": f"ssh {ssh_options}"}}
        # Use depth 1 to bring only the latest revision.
        git_params = ["clone", "--depth", "1", self._repo_url, str(self._REPO_DIR)]
        git.exec_command(*git_params, env=env)  # type: ignore[attr-defined]

    @property
    def repo_url(self):
        """
        Returns the url used to configure this client in `credentials["repo_url"]`.
        """
        return self._repo_url
