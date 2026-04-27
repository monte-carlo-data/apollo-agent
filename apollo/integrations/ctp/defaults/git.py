from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig


class GitClientArgs(TypedDict):
    repo_url: Required[str]
    # HTTPS auth — token used as OAuth2 password; username for Bitbucket-style tokens
    token: NotRequired[str]
    username: NotRequired[str]
    # SSH auth — base64-encoded PEM private key; decoded to bytes by the proxy client
    ssh_key: NotRequired[str]
    # Optional SSL options for HTTPS clones against self-managed Git providers.
    # GitCloneClient honors ca_data (inline PEM CA bundle) and skip_verification /
    # skip_cert_verification. No-op for SSH.
    ssl_options: NotRequired[dict[str, Any]]


GIT_DEFAULT_CTP = CtpConfig(
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
            "ssl_options": "{{ raw.ssl_options | default(none) }}",
        },
    ),
)

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("git", GIT_DEFAULT_CTP)
