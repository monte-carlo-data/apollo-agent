from typing import Any, NotRequired, TypedDict, Union

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep


class HttpClientArgs(TypedDict):
    # Auth
    token: NotRequired[str]
    auth_header: NotRequired[str]  # header name, default "Authorization"
    auth_type: NotRequired[str]  # prefix, e.g. "Bearer" or "Token"
    # SSL — ssl_verify is the resolved value written by the tmp_file_write transform:
    #   True (default) | False (disabled) | str (path to CA bundle file)
    ssl_verify: NotRequired[Union[bool, str]]


HTTP_DEFAULT_CTP = CtpConfig(
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

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("http", HTTP_DEFAULT_CTP)
