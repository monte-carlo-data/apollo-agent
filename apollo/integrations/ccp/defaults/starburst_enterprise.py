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
            type="write_ssl_ca_to_file",
            when="raw.ssl_options is defined and raw.ssl_options.ca_data is defined",
            input={"ssl_options": "{{ raw.ssl_options }}"},
            output={"path": "ssl_ca_path"},
            field_map={"verify": "{{ derived.ssl_ca_path }}"},
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
            # verify=False when SSL disabled; step field_map overrides this when ca_data is present
            "verify": "{{ false if (raw.ssl_options is defined and raw.ssl_options.disabled) else none }}",
        },
    ),
)

from apollo.integrations.ccp.registry import CcpRegistry  # noqa: E402

CcpRegistry.register("starburst-enterprise", STARBURST_ENTERPRISE_DEFAULT_CCP)
