from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep
from apollo.integrations.ctp.registry import CtpRegistry


class PrestoConnectArgs(TypedDict):
    host: Required[str]
    port: Required[int]  # default 8889
    user: NotRequired[str]
    catalog: NotRequired[str]
    schema: NotRequired[str]
    request_timeout: NotRequired[int]
    http_scheme: NotRequired[str]  # "http" or "https"; default "http"
    max_attempts: NotRequired[int]  # default 3
    # auth is a prestodb.auth.BasicAuthentication object produced by resolve_presto_auth.
    # Absent when no auth credentials are provided (when guard on the transform step).
    auth: NotRequired[Any]
    # ssl_options is passed through to the proxy for post-connection _http_session.verify
    # patching, which cannot be expressed in prestodb.dbapi.connect kwargs.
    ssl_options: NotRequired[Any]


PRESTO_DEFAULT_CTP = CtpConfig(
    name="presto-default",
    steps=[
        # Auth: construct BasicAuthentication from raw.auth dict {username, password}.
        # Fires when auth credentials are provided; contributes auth object to connect_args.
        TransformStep(
            type="resolve_presto_auth",
            when="raw.auth is defined",
            input={"auth": "{{ raw.auth }}"},
            output={"auth": "presto_auth_obj"},
            field_map={"auth": "{{ derived.presto_auth_obj }}"},
        ),
        # SSL post-connection verify stays in the proxy client
        # (it patches _http_session.verify, which can't be expressed in connect_args).
    ],
    mapper=MapperConfig(
        name="presto_connect_args",
        schema=PrestoConnectArgs,
        field_map={
            "host": "{{ raw.host }}",
            "port": "{{ raw.port | default(8889) }}",
            "user": "{{ raw.user | default(raw.username) | default(none) }}",
            "catalog": "{{ raw.catalog | default(none) }}",
            "schema": "{{ raw.schema | default(none) }}",
            "request_timeout": "{{ raw.request_timeout | default(none) }}",
            "http_scheme": "{{ raw.http_scheme | default('http') }}",
            "max_attempts": 3,
            # auth is omitted from the mapper — the step above contributes it via
            # field_map when raw.auth is present. When absent, auth is not in connect_args.
            # ssl_options is passed through for proxy-side _http_session.verify patching.
            "ssl_options": "{{ raw.ssl_options | default(none) }}",
        },
    ),
)

CtpRegistry.register("presto", PRESTO_DEFAULT_CTP)
