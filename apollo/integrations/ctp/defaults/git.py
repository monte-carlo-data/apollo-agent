from typing import NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep


class GitClientArgs(TypedDict):
    repo_url: Required[str]
    # HTTPS auth — token used as OAuth2 password; username for Bitbucket-style tokens
    token: NotRequired[str]
    username: NotRequired[str]
    # SSH auth — base64-encoded PEM private key; decoded to bytes by the proxy client
    ssh_key: NotRequired[str]
    # SSL — resolved from raw.ssl_options by the resolve_ssl_options transform.
    # GitCloneClient adds -c http.sslCAInfo=<path> when ssl_ca_path is set, and
    # -c http.sslVerify=false when ssl_skip_verification is True. No-op for SSH.
    ssl_ca_path: NotRequired[str]
    ssl_skip_verification: NotRequired[bool]


GIT_DEFAULT_CTP = CtpConfig(
    name="git-default",
    steps=[
        TransformStep(
            type="resolve_ssl_options",
            when="raw.ssl_options is defined",
            input={"ssl_options": "{{ raw.ssl_options }}"},
            output={
                "ssl_options": "ssl_options",  # derived.ssl_options for condition access
                "ca_path": "ssl_ca_path",  # derived.ssl_ca_path if ca_data was written
            },
        ),
    ],
    mapper=MapperConfig(
        name="git_client_args",
        schema=GitClientArgs,
        field_map={
            "repo_url": "{{ raw.repo_url }}",
            "token": "{{ raw.token | default(none) }}",
            "username": "{{ raw.username | default(none) }}",
            "ssh_key": "{{ raw.ssh_key | default(none) }}",
            "ssl_ca_path": "{{ derived.ssl_ca_path | default(none) }}",
            "ssl_skip_verification": (
                "{{ true if derived.ssl_options is defined "
                "and derived.ssl_options.skip_cert_verification else none }}"
            ),
        },
    ),
)

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("git", GIT_DEFAULT_CTP)
