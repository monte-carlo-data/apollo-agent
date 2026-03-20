from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig, TransformStep


class MysqlClientArgs(TypedDict):
    host: Required[str]
    port: Required[str]
    user: Required[str]
    password: Required[str]
    ssl: NotRequired[
        Any
    ]  # dict {"ca": path} for remote cert, or ssl.SSLContext for inline data


MYSQL_DEFAULT_CCP = CcpConfig(
    name="mysql-default",
    steps=[
        # Remote CA: download cert from URL/storage, pass as {"ca": path}
        TransformStep(
            type="fetch_remote_file",
            when="raw.ssl_options is defined and raw.ssl_options.ca is defined",
            input={
                "url": "{{ raw.ssl_options.ca }}",
                "sub_folder": "mysql",
                "mechanism": "{{ raw.ssl_options.mechanism | default('url') }}",
            },
            output={"path": "ssl_ca_path"},
            field_map={"ssl": "{{ {'ca': derived.ssl_ca_path} }}"},
        ),
        # Inline cert data: build SSLContext from ca_data / cert_data / key_data
        TransformStep(
            type="resolve_ssl_options",
            when="raw.ssl_options is defined and raw.ssl_options.ca is not defined",
            input={"ssl_options": "{{ raw.ssl_options }}"},
            output={"ssl_context": "ssl_context"},
            field_map={
                "ssl": "{{ derived.ssl_context if derived.ssl_context is defined else none }}"
            },
        ),
    ],
    mapper=MapperConfig(
        name="mysql_client_args",
        schema=MysqlClientArgs,
        field_map={
            "host": "{{ raw.host }}",
            "port": "{{ raw.port }}",
            "user": "{{ raw.user }}",
            "password": "{{ raw.password }}",
        },
    ),
)

from apollo.integrations.ccp.registry import CcpRegistry  # noqa: E402

CcpRegistry.register("mysql", MYSQL_DEFAULT_CCP)
