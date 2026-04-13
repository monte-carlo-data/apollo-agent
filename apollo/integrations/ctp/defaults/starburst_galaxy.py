from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig, TransformStep


class StarburstGalaxyClientArgs(TypedDict):
    # Network
    host: Required[str]
    port: NotRequired[int]
    http_scheme: NotRequired[str]  # "http" | "https"
    # Auth — user/password are converted to BasicAuthentication by the proxy client
    user: NotRequired[str]
    password: NotRequired[str]
    auth: NotRequired[Any]  # trino.auth.* object if pre-built
    extra_credential: NotRequired[dict]
    # Database
    catalog: NotRequired[str]  # default "hive"
    schema: NotRequired[str]  # default "default"
    source: NotRequired[str]
    # Session
    session_properties: NotRequired[dict]
    roles: NotRequired[list]
    timezone: NotRequired[str]
    client_tags: NotRequired[list]
    # HTTP
    http_headers: NotRequired[dict]
    http_session: NotRequired[Any]  # custom requests.Session
    # Timeouts / retries
    request_timeout: NotRequired[float]
    max_attempts: NotRequired[int]
    # SSL
    verify: NotRequired[Any]  # True (default) | False | str (cert path)
    # Transactions
    isolation_level: NotRequired[Any]  # trino.transaction.IsolationLevel
    # Compatibility
    legacy_primitive_types: NotRequired[bool]
    legacy_prepared_statements: NotRequired[bool]
    encoding: NotRequired[Any]


STARBURST_GALAXY_DEFAULT_CTP = CtpConfig(
    name="starburst-galaxy-default",
    steps=[
        TransformStep(
            type="resolve_ssl_options",
            when="raw.ssl_options is defined",
            input={"ssl_options": "{{ raw.ssl_options }}"},
            output={
                "ssl_options": "ssl_options",  # derived.ssl_options for condition access
                "ca_path": "ssl_ca_path",  # derived.ssl_ca_path if ca_data written
            },
            field_map={
                # cert path if ca_data was written, False if disabled, absent otherwise
                "verify": "{{ derived.ssl_ca_path if derived.ssl_ca_path is defined else (false if derived.ssl_options.disabled else none) }}",
            },
        )
    ],
    mapper=MapperConfig(
        name="starburst_galaxy_client_args",
        schema=StarburstGalaxyClientArgs,
        field_map={
            "host": "{{ raw.host }}",
            "port": "{{ raw.port }}",  # DC casts to int: int(port or 443); mapper coerces str→int
            "user": "{{ raw.user }}",
            "password": "{{ raw.password }}",
            "http_scheme": "https",
            "catalog": "{{ raw.catalog | default(none) }}",
            "schema": "{{ raw.schema | default(none) }}",
        },
    ),
)
