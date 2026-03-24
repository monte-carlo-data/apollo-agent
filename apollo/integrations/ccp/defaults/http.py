from typing import Any, NotRequired, TypedDict, Union

from apollo.integrations.ccp.models import CcpConfig, MapperConfig, TransformStep

# NOTE: This config is intentionally NOT registered in CcpRegistry._discover().
#
# HttpProxyClient reads credentials flat (credentials.get("token"), etc.) rather
# than from credentials["connect_args"]. DC also sends flat credentials for http
# with no connect_args wrapper, so the legacy short-circuit in CcpRegistry.resolve()
# does not protect it.
#
# If this config were registered today, resolve() would wrap output in
# {"connect_args": {...}} and the proxy client would silently lose all credentials.
#
# Phase 2 work required before registering:
#   1. Update HttpProxyClient.__init__ to read token/auth_header/auth_type from
#      credentials["connect_args"] instead of the top-level credentials dict.
#   2. Remove the ssl_options handling from HttpProxyClient.__init__ — the
#      tmp_file_write transform below takes over cert materialisation.
#   3. Add `import apollo.integrations.ccp.defaults.http` to CcpRegistry._discover().


class HttpClientArgs(TypedDict):
    # Auth
    token: NotRequired[str]
    auth_header: NotRequired[str]  # header name, default "Authorization"
    auth_type: NotRequired[str]  # prefix, e.g. "Bearer" or "Token"
    # SSL — ssl_verify is the resolved value written by the tmp_file_write transform:
    #   True (default) | False (disabled) | str (path to CA bundle file)
    ssl_verify: NotRequired[Union[bool, str]]


HTTP_DEFAULT_CCP = CcpConfig(
    name="http-default",
    steps=[
        TransformStep(
            type="tmp_file_write",
            when="raw.ssl_options is defined and raw.ssl_options.ca_data is defined",
            input={
                "contents": "{{ raw.ssl_options.ca_data }}",
                "file_suffix": ".pem",
                "mode": "0600",
            },
            output={"path": "ssl_ca_path"},
            field_map={
                "ssl_verify": "{{ derived.ssl_ca_path }}",
            },
        ),
    ],
    mapper=MapperConfig(
        name="http_client_args",
        schema=HttpClientArgs,
        field_map={
            "token": "{{ raw.token | default(none) }}",
            "auth_header": "{{ raw.auth_header | default(none) }}",
            "auth_type": "{{ raw.auth_type | default(none) }}",
            "ssl_verify": "{{ false if raw.ssl_options is defined and raw.ssl_options.disabled else none }}",
        },
    ),
)

# Intentionally not registered — see module docstring above.
# from apollo.integrations.ccp.registry import CcpRegistry  # noqa: E402
# CcpRegistry.register("http", HTTP_DEFAULT_CCP)
