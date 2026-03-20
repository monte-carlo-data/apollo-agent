from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig


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


STARBURST_GALAXY_DEFAULT_CCP = CcpConfig(
    name="starburst-galaxy-default",
    steps=[],
    mapper=MapperConfig(
        name="starburst_galaxy_client_args",
        schema=StarburstGalaxyClientArgs,
        field_map={
            "host": "{{ raw.host }}",
            "port": "{{ raw.port | int }}",  # DC casts to int: int(port or 443)
            "user": "{{ raw.user }}",
            "password": "{{ raw.password }}",
            "http_scheme": "https",
            "catalog": "{{ raw.catalog | default(none) }}",
            "schema": "{{ raw.schema | default(none) }}",
            "source": "{{ raw.source | default(none) }}",
            "session_properties": "{{ raw.session_properties | default(none) }}",
            "client_tags": "{{ raw.client_tags | default(none) }}",
            "request_timeout": "{{ raw.request_timeout | default(none) }}",
            "max_attempts": "{{ raw.max_attempts | default(none) }}",
        },
    ),
)

from apollo.integrations.ccp.registry import CcpRegistry  # noqa: E402

CcpRegistry.register("starburst-galaxy", STARBURST_GALAXY_DEFAULT_CCP)
