from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig, TransformStep


class MysqlClientArgs(TypedDict):
    # Network
    host: NotRequired[str]
    port: NotRequired[int]  # default 3306
    unix_socket: NotRequired[str]
    bind_address: NotRequired[str]
    # Auth
    user: NotRequired[str]
    password: NotRequired[str]
    server_public_key: NotRequired[bytes]
    auth_plugin_map: NotRequired[dict]
    # Database
    database: NotRequired[str]
    sql_mode: NotRequired[str]
    # Charset
    charset: NotRequired[str]  # default "utf8mb4"
    collation: NotRequired[str]
    use_unicode: NotRequired[bool]  # default True
    # Timeouts
    connect_timeout: NotRequired[int]  # default 10 seconds
    read_timeout: NotRequired[int]
    write_timeout: NotRequired[int]
    # SSL — CCP resolves ssl_options into this field via transform steps;
    # native ssl_* fields below are valid if passed directly in connect_args
    ssl: NotRequired[Any]  # dict {"ca": path} or ssl.SSLContext
    ssl_ca: NotRequired[str]
    ssl_cert: NotRequired[str]
    ssl_key: NotRequired[str]
    ssl_key_password: NotRequired[str]
    ssl_disabled: NotRequired[bool]
    ssl_verify_cert: NotRequired[bool]
    ssl_verify_identity: NotRequired[bool]
    # Session
    autocommit: NotRequired[bool]  # default False
    init_command: NotRequired[str]
    # Packets / connection options
    max_allowed_packet: NotRequired[int]  # default 16 MB
    local_infile: NotRequired[bool]
    client_flag: NotRequired[int]
    program_name: NotRequired[str]
    defer_connect: NotRequired[bool]
    # Config file
    read_default_file: NotRequired[str]
    read_default_group: NotRequired[str]
    # Cursor / data handling
    conv: NotRequired[dict]
    binary_prefix: NotRequired[bool]


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
            field_map={
                "ssl": "{{ {'ca': derived.ssl_ca_path} if derived.ssl_ca_path is defined else none }}"
            },
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

