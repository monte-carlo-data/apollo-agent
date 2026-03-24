from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig, TransformStep


class PrestoConnectArgs(TypedDict):
    host: Required[str]
    port: Required[int]  # default 8889
    user: NotRequired[str]
    catalog: NotRequired[str]
    schema: NotRequired[str]
    request_timeout: NotRequired[int]
    http_scheme: NotRequired[str]  # "http" or "https"; default "http"
    max_attempts: NotRequired[int]  # default 3
    # auth is a dict {username, password} in the CCP output.
    # The proxy client pops it and wraps it in prestodb.auth.BasicAuthentication(**auth).
    # Phase 2 will add a resolve_presto_auth transform that produces the object directly.
    # Note: the proxy client uses connect_args.pop("auth") without a default, so auth
    # must always be present in connect_args (even as None/falsy). Phase 2 will update
    # the proxy client to use pop("auth", None) to remove this constraint.
    auth: NotRequired[Any]


PRESTO_DEFAULT_CCP = CcpConfig(
    name="presto-default",
    steps=[
        # Auth: construct BasicAuthentication from raw.auth dict {username, password}.
        # Fires when auth credentials are provided; contributes auth object to connect_args.
        # Phase 2 will also update the proxy client to skip its own re-wrapping when
        # connect_args["auth"] is already a BasicAuthentication object.
        TransformStep(
            type="resolve_presto_auth",
            when="raw.auth is defined",
            input={"auth": "{{ raw.auth }}"},
            output={"auth": "presto_auth_obj"},
            field_map={"auth": "{{ derived.presto_auth_obj }}"},
        ),
        # Phase 2: SSL post-connection verify stays in the proxy client
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
            # field_map when raw.auth is present. When absent, auth is not in
            # connect_args. Phase 2 updates proxy client to use pop("auth", None).
        },
    ),
)

# Not registered: two Phase 2 blockers —
# 1. SSL uses credentials["ssl_options"] via http_session.verify; CCP output drops it.
# 2. proxy client uses connect_args.pop("auth") without a default; mapper omits None
#    values so auth would be absent when not provided, raising KeyError.
# Phase 2 will add resolve_presto_auth transform, update the proxy client, and register.
