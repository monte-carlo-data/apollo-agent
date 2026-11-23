import base64
import logging
import os
import re
import shlex
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Dict, List, Generator, Optional, Set

import git

logger = logging.getLogger(__name__)

# Pinned host keys for the public Git hosts, from each provider's published list
# (see the header of the file). Shipped inside the package so every runtime that
# copies apollo/ (generic, cloudrun, aws, lambda, azure) has it.
BUNDLED_KNOWN_HOSTS_PATH = Path(__file__).with_name("known_hosts")

# scp-like Git URL: [user@]host:path. The host may be bracketed ([host]:path).
_SCP_LIKE_URL = re.compile(r"^(?:[^@/]+@)?(?P<host>\[[^\]]+\]|[^:/\[\]]+):(?!//)")

# lambda-git logs every command at INFO, and HTTPS clone commands carry the token in the URL.
logging.getLogger("git").setLevel(logging.WARNING)


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

    SSH clones verify the server's host key (SEC-4040):

    - Hosts in the bundled ``known_hosts`` (github.com, gitlab.com, bitbucket.org and
      their alternate SSH endpoints) always use ``StrictHostKeyChecking=yes`` against
      the pinned keys. They are never trusted on first use.
    - ``ssh_known_hosts`` (str, optional): one or more OpenSSH ``known_hosts`` lines
      for a self-hosted Git server. When set, the clone also runs with
      ``StrictHostKeyChecking=yes`` and fails if the server's key is not listed.
    - Any other host (self-hosted, no ``ssh_known_hosts`` configured) keeps working
      as before, with a warning that its host key is not verified. Configuring
      ``ssh_known_hosts`` on the connection moves it to strict checking.
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
        self._ssh_known_hosts: str = (creds.get("ssh_known_hosts") or "").strip()

        # Unique per-client paths (only /tmp is writable in lambda). A fixed path
        # would collide when git operations run concurrently — possible on
        # long-lived runtimes (EKS/hermes, Azure) even if not on Lambda/Cloud Run —
        # letting one request overwrite/delete another's repo or SSH key mid-clone.
        # The repo dir name is unguessable and created by `git clone`; the SSH key
        # file is created per-write via mkstemp (see write_key).
        self._repo_dir = Path(tempfile.gettempdir()) / f"mcd_repo_{uuid.uuid4().hex}"
        self._key_file: Path | None = None
        self._known_hosts_file: Path | None = None

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
            self.delete_known_hosts()

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

    def delete_known_hosts(self):
        """Delete the per-clone known_hosts file if it exists."""
        if self._known_hosts_file and self._known_hosts_file.exists():
            self._known_hosts_file.unlink()

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

            raise git.exceptions.GitExecutionError(password_removed_message) from None  # type: ignore[attr-defined]

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

    @staticmethod
    def ssh_known_hosts_name(repo_url: str) -> Optional[str]:
        """
        Return the name OpenSSH looks up in known_hosts for an SSH repo URL:
        ``host`` on port 22, ``[host]:port`` otherwise, lower-cased. None if the URL
        is not an SSH URL this client can parse.
        """
        url = repo_url.strip()
        host: Optional[str] = None
        port: Optional[int] = None
        if url.startswith("ssh://"):
            authority = url[len("ssh://") :].split("/", 1)[0].rsplit("@", 1)[-1]
            if authority.startswith("["):
                host, _, rest = authority[1:].partition("]")
                if rest.startswith(":") and rest[1:].isdigit():
                    port = int(rest[1:])
            else:
                host, _, port_str = authority.partition(":")
                if port_str.isdigit():
                    port = int(port_str)
        else:
            match = _SCP_LIKE_URL.match(url)
            if match:
                host = match.group("host").strip("[]")
        if not host:
            return None
        host = host.lower()
        if port is None or port == 22:
            return host
        return f"[{host}]:{port}"

    @staticmethod
    def pinned_host_names() -> Set[str]:
        """Host names that have pinned keys in the bundled known_hosts."""
        names: Set[str] = set()
        for line in BUNDLED_KNOWN_HOSTS_PATH.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            names.update(n.lower() for n in line.split()[0].split(","))
        return names

    def write_known_hosts(self) -> Path:
        """
        Write a per-clone known_hosts: the bundled pinned keys plus any
        ``ssh_known_hosts`` lines configured on the connection. It is a fresh 0o600
        temp file so concurrent clones never share or modify each other's file.
        """
        fd, path = tempfile.mkstemp(prefix="mcd_known_hosts_")
        os.close(fd)
        self._known_hosts_file = Path(path)
        content = BUNDLED_KNOWN_HOSTS_PATH.read_text()
        if self._ssh_known_hosts:
            content = f"{content.rstrip()}\n{self._ssh_known_hosts}\n"
        self._known_hosts_file.write_text(content)
        self._known_hosts_file.chmod(0o600)
        return self._known_hosts_file

    def ssh_strict_host_key_checking(self) -> str:
        """
        ``yes`` for pinned public hosts and for connections with ``ssh_known_hosts``.
        ``accept-new`` otherwise: the host is not in the known_hosts file, so its key
        is accepted for this clone only (the file is deleted afterwards). Because the
        pinned hosts are in the file, ``accept-new`` still rejects a wrong key for them.
        """
        host_name = self.ssh_known_hosts_name(self._repo_url)
        if self._ssh_known_hosts or (
            host_name is not None and host_name in self.pinned_host_names()
        ):
            return "yes"
        logger.warning(
            "SSH host key for %s is not verified: no known_hosts entry is configured "
            "on this Git connection (ssh_known_hosts).",
            host_name or "the Git server",
        )
        return "accept-new"

    def ssh_command(self) -> str:
        """Build GIT_SSH_COMMAND. Requires write_key() to have run."""
        known_hosts = self.write_known_hosts()
        ssh_options = [
            "-v",  # Verbose helps in case of problems
            f"-o StrictHostKeyChecking={self.ssh_strict_host_key_checking()}",
            # Only the per-clone file: pinned keys plus the connection's own keys.
            f"-o UserKnownHostsFile={shlex.quote(str(known_hosts))}",
            "-o GlobalKnownHostsFile=/dev/null",
            "-o UpdateHostKeys=no",
            f"-i {shlex.quote(str(self._key_file))}",  # Use this key
        ]
        return f"ssh {' '.join(ssh_options)}"

    def _ssh_git_clone(self):
        """
        Clone a git repo. It uses GIT_SSH_COMMAND to pass the SSH key and the host
        key checking options (see the class docstring).

        "repo_url" can be a full ssh URL (ssh://[user@]server/project.git) or the shorter version
        ([user@]server:project.git).
        """
        logger.info(f"Clone repo: {self._repo_url}")
        env = {**os.environ, **{"GIT_SSH_COMMAND": self.ssh_command()}}
        # Use depth 1 to bring only the latest revision.
        git_params = ["clone", "--depth", "1", self._repo_url, str(self._repo_dir)]
        git.exec_command(*git_params, env=env)  # type: ignore[attr-defined]

    @property
    def repo_url(self):
        """
        Returns the url used to configure this client in `credentials["repo_url"]`.
        """
        return self._repo_url
