"""
Regenerate apollo/integrations/git/known_hosts from each provider's published
SSH host keys (SEC-4040). Run it when GitHub, GitLab or Bitbucket rotates a key,
then review the diff and the fingerprints it prints:

    python scripts/update_git_known_hosts.py

Sources are the providers' own publications, never ssh-keyscan:
- GitHub: https://api.github.com/meta (ssh_keys); the same keys serve ssh.github.com:443.
- GitLab.com: the "SSH known_hosts entries" block of the GitLab.com docs.
- Bitbucket Cloud: https://bitbucket.org/site/ssh (known_hosts lines).
"""

import json
import re
import subprocess
import sys
import urllib.request
from datetime import date
from pathlib import Path

KNOWN_HOSTS = (
    Path(__file__).resolve().parent.parent / "apollo/integrations/git/known_hosts"
)
GITHUB_META = "https://api.github.com/meta"
GITLAB_DOC = (
    "https://gitlab.com/gitlab-org/gitlab/-/raw/master/doc/user/gitlab_com/_index.md"
)
BITBUCKET_SSH = "https://bitbucket.org/site/ssh"
KEY_TYPES = ("ssh-ed25519", "ecdsa-sha2-nistp256", "ssh-rsa")


def _get(url: str) -> str:
    with urllib.request.urlopen(
        url, timeout=30
    ) as resp:  # noqa: S310 - fixed https URLs
        return resp.read().decode("utf-8")


def _github() -> list[str]:
    keys = json.loads(_get(GITHUB_META))["ssh_keys"]
    return [f"github.com,[ssh.github.com]:443 {k}" for k in keys]


def _gitlab() -> list[str]:
    lines = re.findall(r"^gitlab\.com \S+ \S+$", _get(GITLAB_DOC), re.MULTILINE)
    return [line for line in lines if line.split()[1] in KEY_TYPES]


def _bitbucket() -> list[str]:
    lines = [line.strip() for line in _get(BITBUCKET_SSH).splitlines()]
    return [
        line
        for line in lines
        if line.split()[:1] in (["bitbucket.org"], ["ssh.bitbucket.org"])
    ]


def main() -> int:
    entries = {"github": _github(), "gitlab": _gitlab(), "bitbucket": _bitbucket()}
    for name, lines in entries.items():
        if not lines:
            print(f"no keys found for {name}; refusing to write", file=sys.stderr)
            return 1
    header = KNOWN_HOSTS.read_text().split("\ngithub.com", 1)[0]
    header = re.sub(r"Fetched \d{4}-\d{2}-\d{2}", f"Fetched {date.today()}", header)
    body = [line for lines in entries.values() for line in lines]
    KNOWN_HOSTS.write_text(header.rstrip("\n") + "\n" + "\n".join(body) + "\n")
    subprocess.run(["ssh-keygen", "-lf", str(KNOWN_HOSTS)], check=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
