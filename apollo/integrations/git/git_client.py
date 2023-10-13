import base64
import logging
import os
import re
import shutil
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
    # Only /tmp is writable in lambda.
    _REPO_DIR = Path("/tmp/repo")
    _KEY_FILE = Path("/tmp/mcd_rsa")

    GIT_TYPE = {"ssh": "SSH", "https": "HTTPS"}

    def __init__(self, credentials: Dict, **kwargs):  # type: ignore
        self._repo_url = credentials["repo_url"]
        self._token = credentials.get("token")
        self._username = credentials.get("username")
        self._ssh_key = base64.b64decode(credentials.get("ssh_key", ""))

        if self._token:
            # remove https if it was included for https calls
            self._repo_url = self._repo_url.lstrip("https://")

    def get_files(
        self, file_extensions: List[str]
    ) -> Generator[GitFileData, None, None]:
        """Main method, read the content of all files."""
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

    def git_version(self) -> Dict:
        stdout, stderr = git.exec_command("--version")
        return {
            "stdout": stdout.decode("utf-8") if stdout else "",
            "stderr": stderr.decode("utf-8") if stderr else "",
        }

    def git_clone(self):
        """
        Clone a git repo.

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
        Clone a git repo. It uses Https and a git authoriztion token.

        "repo_url" can be a full git https URL (https://server/project.git) or the shorter version
        (server/project.git).
        """
        logger.info(f"Clone repo: {self._repo_url}")
        url = f"https://oauth2:{self._token}@{self._repo_url}"
        if self._username:
            # This allows for support of bitbucket as they handle access tokens slightly differently
            url = f"https://{self._username}:{self._token}@{self._repo_url}"
        # Use depth 1 to bring only the latest revision.
        git_params = ["clone", "--depth", "1", url, str(self._REPO_DIR)]
        try:
            git.exec_command(*git_params)
        except git.exceptions.GitExecutionError as e:
            password_removed_message = self._replace_passwords_in_urls(str(e))

            raise git.exceptions.GitExecutionError(password_removed_message)

    def _replace_passwords_in_urls(self, text: str, placeholder: str = "********"):
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
        git.exec_command(*git_params, env=env)

    @property
    def repo_url(self):
        return self._repo_url
