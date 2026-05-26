from typing import NotRequired, Required, TypedDict

from apollo.credentials.schema.common import SSL_OPTIONS_FIELD
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


# Customer-facing self-hosted credentials match the Looker-GIT docs accordion:
# repo_url + ssh_key. The CTP also supports HTTPS-token auth on the DC pre-shape
# path, but customer self-hosted JSON for git/Looker-GIT uses SSH per docs.
GIT_CREDENTIALS_SCHEMA = {
    "repo_url": {"type": "string", "required": True, "empty": False},
    "ssh_key": {"type": "string", "required": True, "empty": False},
    "ssl_options": SSL_OPTIONS_FIELD,
}

GIT_DEFAULT_CTP = CtpConfig(
    name="git-default",
    raw_credentials_schema=GIT_CREDENTIALS_SCHEMA,
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
