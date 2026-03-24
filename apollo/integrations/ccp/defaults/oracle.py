from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig


class OracleClientArgs(TypedDict):
    # Connection — use dsn string OR individual host/port/service_name/sid
    dsn: NotRequired[str]
    host: NotRequired[str]
    port: NotRequired[int]  # default 1521
    service_name: NotRequired[str]
    sid: NotRequired[str]
    protocol: NotRequired[str]  # "tcp" (default) | "tcps"
    server_type: NotRequired[str]  # "dedicated" | "shared" | "pooled"
    # Auth
    user: NotRequired[str]
    password: NotRequired[str]
    proxy_user: NotRequired[str]
    newpassword: NotRequired[str]
    externalauth: NotRequired[bool]
    access_token: NotRequired[Any]  # str | tuple | callable — OAuth 2.0 / OCI IAM
    mode: NotRequired[Any]  # oracledb.AuthMode
    # Wallet / SSL
    wallet_location: NotRequired[str]
    wallet_password: NotRequired[str]
    ssl_context: NotRequired[Any]  # ssl.SSLContext
    ssl_server_dn_match: NotRequired[bool]  # default True
    ssl_server_cert_dn: NotRequired[str]
    ssl_version: NotRequired[Any]  # ssl.TLSVersion
    # Network
    tcp_connect_timeout: NotRequired[float]  # default 20.0 seconds
    https_proxy: NotRequired[str]
    https_proxy_port: NotRequired[int]
    use_tcp_fast_open: NotRequired[bool]
    # Connection behaviour
    expire_time: NotRequired[int]  # keepalive interval in minutes; default 0 (disabled)
    retry_count: NotRequired[int]
    retry_delay: NotRequired[int]  # seconds; default 1
    disable_oob: NotRequired[bool]
    sdu: NotRequired[int]  # Session Data Unit bytes; default 8192
    # Statement cache
    stmtcachesize: NotRequired[int]
    # Session / DRCP
    cclass: NotRequired[str]
    purity: NotRequired[Any]  # oracledb.Purity
    pool_boundary: NotRequired[str]
    edition: NotRequired[str]
    events: NotRequired[bool]
    # Client identification
    program: NotRequired[str]
    machine: NotRequired[str]
    terminal: NotRequired[str]
    osuser: NotRequired[str]
    driver_name: NotRequired[str]
    connection_id_prefix: NotRequired[str]
    debug_jdwp: NotRequired[str]  # "host=<host>;port=<port>"


ORACLE_DEFAULT_CCP = CcpConfig(
    name="oracle-default",
    steps=[],
    mapper=MapperConfig(
        name="oracle_client_args",
        schema=OracleClientArgs,
        field_map={
            "dsn": "{{ raw.dsn | default(none) }}",
            "user": "{{ raw.user | default(none) }}",
            "password": "{{ raw.password | default(none) }}",
            # Proxy client default is 1 (keepalive every minute); CCP matches that behaviour
            "expire_time": "{{ raw.expire_time | default(1) }}",
        },
    ),
)
