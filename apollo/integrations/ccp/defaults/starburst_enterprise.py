from typing import Any, NotRequired, Required, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig, TransformStep


class StarburstEnterpriseClientArgs(TypedDict):
    host: Required[str]
    port: Required[int]
    user: Required[str]
    password: Required[str]
    http_scheme: Required[str]
    verify: NotRequired[Any]  # str (cert path) or False


STARBURST_ENTERPRISE_DEFAULT_CCP = CcpConfig(
    name="starburst-enterprise-default",
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
        name="starburst_enterprise_client_args",
        schema=StarburstEnterpriseClientArgs,
        field_map={
            "host": "{{ raw.host }}",
            "port": "{{ raw.port | int }}",  # DC input has port as string e.g. "8443"
            "user": "{{ raw.user }}",
            "password": "{{ raw.password }}",
            "http_scheme": "https",
        },
    ),
)

from apollo.integrations.ccp.registry import CcpRegistry  # noqa: E402

CcpRegistry.register("starburst-enterprise", STARBURST_ENTERPRISE_DEFAULT_CCP)
