from typing import Any, NotRequired, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig


class SalesforceCrmClientArgs(TypedDict):
    # Token-based auth
    username: NotRequired[str]  # ← raw.user
    password: NotRequired[str]
    security_token: NotRequired[str]
    # Connected App / OAuth auth
    consumer_key: NotRequired[str]
    consumer_secret: NotRequired[str]
    domain: NotRequired[
        str
    ]  # org subdomain only, e.g. "myorg" not "myorg.salesforce.com"
    # Connection options
    instance_url: NotRequired[str]  # direct instance URL, bypasses login
    session_id: NotRequired[str]  # pre-existing session token
    version: NotRequired[str]  # Salesforce API version, e.g. "59.0"
    sandbox: NotRequired[bool]
    client_id: NotRequired[str]  # client name sent in X-Chatter-Entity-Encoding header
    proxies: NotRequired[dict]
    session: NotRequired[Any]  # requests.Session


SALESFORCE_CRM_DEFAULT_CCP = CcpConfig(
    name="salesforce-crm-default",
    steps=[],
    mapper=MapperConfig(
        name="salesforce_crm_client_args",
        schema=SalesforceCrmClientArgs,
        field_map={
            # Token auth
            "username": "{{ raw.user | default(none) }}",
            "password": "{{ raw.password | default(none) }}",
            "security_token": "{{ raw.security_token | default(none) }}",
            # OAuth auth
            "consumer_key": "{{ raw.consumer_key | default(none) }}",
            "consumer_secret": "{{ raw.consumer_secret | default(none) }}",
            # Strip ".salesforce.com" suffix if present — simple_salesforce expects the subdomain only
            "domain": "{{ raw.domain | replace('.salesforce.com', '') | default(none) if raw.domain is defined else none }}",
        },
    ),
)

from apollo.integrations.ccp.registry import CcpRegistry  # noqa: E402

CcpRegistry.register("salesforce-crm", SALESFORCE_CRM_DEFAULT_CCP)
