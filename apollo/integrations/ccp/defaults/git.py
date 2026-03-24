from typing import NotRequired, Required, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig

# NOTE: This config is intentionally NOT registered in CcpRegistry._discover().
#
# GitProxyClient (and GitCloneClient) read credentials flat — credentials["repo_url"],
# credentials.get("token"), etc. — rather than from credentials["connect_args"]. DC
# also sends flat credentials for git with no connect_args wrapper, so the legacy
# short-circuit in CcpRegistry.resolve() does not protect it.
#
# If this config were registered today, resolve() would wrap output in
# {"connect_args": {...}} and the proxy client would immediately raise ValueError
# ("Credentials are required for Git" / "repo_url" KeyError).
#
# Phase 2 work required before registering:
#   1. Update GitCloneClient.__init__ to read repo_url/token/username/ssh_key from
#      credentials["connect_args"] instead of the top-level credentials dict.
#   2. Add `import apollo.integrations.ccp.defaults.git` to CcpRegistry._discover().
#
# Invariant proxy client logic that stays in GitCloneClient (not CCP concerns):
#   - base64.b64decode(ssh_key) — always decode, no branching
#   - repo_url.lstrip("https://") when token present — URL construction detail


class GitClientArgs(TypedDict):
    repo_url: Required[str]
    # HTTPS auth — token used as OAuth2 password; username for Bitbucket-style tokens
    token: NotRequired[str]
    username: NotRequired[str]
    # SSH auth — base64-encoded PEM private key; decoded to bytes by the proxy client
    ssh_key: NotRequired[str]


GIT_DEFAULT_CCP = CcpConfig(
    name="git-default",
    steps=[],
    mapper=MapperConfig(
        name="git_client_args",
        schema=GitClientArgs,
        field_map={
            "repo_url": "{{ raw.repo_url }}",
            "token": "{{ raw.token | default(none) }}",
            "username": "{{ raw.username | default(none) }}",
            "ssh_key": "{{ raw.ssh_key | default(none) }}",
        },
    ),
)

# Intentionally not registered — see module docstring above.
# from apollo.integrations.ccp.registry import CcpRegistry  # noqa: E402
# CcpRegistry.register("git", GIT_DEFAULT_CCP)
