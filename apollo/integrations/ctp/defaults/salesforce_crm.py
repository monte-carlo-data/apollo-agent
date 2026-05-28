from typing import Any, NotRequired, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig


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


# Salesforce CRM supports two auth modes per docs (token vs OAuth). Each is
# a fully-specified variant under oneof_schema; cerberus rejects ambiguous
# combinations (e.g. supplying both consumer_key AND security_token).
SALESFORCE_CRM_CREDENTIALS_SCHEMA = {
    "connect_args": {
        "type": "dict",
        "required": True,
        "oneof_schema": [
            # Token auth — docs spelling is `user`.
            {
                "user": {"type": "string", "required": True, "empty": False},
                "password": {"type": "string", "required": True, "empty": False},
                "security_token": {
                    "type": "string",
                    "required": True,
                    "empty": False,
                },
            },
            # Token auth — `username` alternate accepted by the CTP and used
            # by some customers.
            {
                "username": {"type": "string", "required": True, "empty": False},
                "password": {"type": "string", "required": True, "empty": False},
                "security_token": {
                    "type": "string",
                    "required": True,
                    "empty": False,
                },
            },
            # Connected App / OAuth.
            {
                "consumer_key": {
                    "type": "string",
                    "required": True,
                    "empty": False,
                },
                "consumer_secret": {
                    "type": "string",
                    "required": True,
                    "empty": False,
                },
                "domain": {"type": "string", "required": True, "empty": False},
            },
        ],
    },
}

SALESFORCE_CRM_DEFAULT_CTP = CtpConfig(
    name="salesforce-crm-default",
    raw_credentials_schema=SALESFORCE_CRM_CREDENTIALS_SCHEMA,
    steps=[],
    mapper=MapperConfig(
        name="salesforce_crm_client_args",
        schema=SalesforceCrmClientArgs,
        field_map={
            # Token auth
            "username": "{{ raw.user | default(raw.username) | default(none) }}",
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

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("salesforce-crm", SALESFORCE_CRM_DEFAULT_CTP)
