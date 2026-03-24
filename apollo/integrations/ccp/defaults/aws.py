from typing import NotRequired, TypedDict

from apollo.integrations.ccp.models import CcpConfig, MapperConfig

# NOTE: These configs are intentionally NOT registered in CcpRegistry._discover().
#
# BaseAwsProxyClient reads credentials flat — credentials.get("assumable_role"),
# credentials.get("aws_region"), etc. — rather than from credentials["connect_args"].
# DC also sends flat credentials for all AWS services with no connect_args wrapper,
# so the legacy short-circuit in CcpRegistry.resolve() does not protect them.
#
# If any config were registered today, resolve() would wrap output in
# {"connect_args": {...}} and BaseAwsProxyClient.__init__ would receive None for
# all fields (credentials.get("assumable_role") on {"connect_args": {...}} → None).
#
# Phase 2 work required before registering any AWS config:
#   1. Update BaseAwsProxyClient.__init__ to accept credentials["connect_args"] and
#      read assumable_role/aws_region/external_id/ssl_options from there instead of
#      from the top-level credentials dict. One change covers all five clients.
#   2. Add AWS config imports to CcpRegistry._discover().
#
# Invariant proxy client logic that stays in BaseAwsProxyClient (not CCP concerns):
#   - boto3.Session creation and IAM role assumption via STS
#   - SslOptions(**ssl_options).write_ca_data_to_temp_file() — CA bundle materialisation
#     happens inside create_boto_client; ssl_options is passed through as-is by CCP


class AwsClientArgs(TypedDict):
    # IAM role assumption — optional; if absent, uses ambient boto3 credentials
    assumable_role: NotRequired[str]
    aws_region: NotRequired[str]
    external_id: NotRequired[str]  # ExternalId for cross-account assume-role
    # SSL — passed as-is to SslOptions(**ssl_options); CA bundle written by proxy client
    ssl_options: NotRequired[dict]


_AWS_FIELD_MAP = {
    "assumable_role": "{{ raw.assumable_role | default(none) }}",
    "aws_region": "{{ raw.aws_region | default(none) }}",
    "external_id": "{{ raw.external_id | default(none) }}",
    "ssl_options": "{{ raw.ssl_options | default(none) }}",
}

ATHENA_DEFAULT_CCP = CcpConfig(
    name="athena-default",
    steps=[],
    mapper=MapperConfig(
        name="aws_client_args",
        schema=AwsClientArgs,
        field_map=_AWS_FIELD_MAP,
    ),
)

GLUE_DEFAULT_CCP = CcpConfig(
    name="glue-default",
    steps=[],
    mapper=MapperConfig(
        name="aws_client_args",
        schema=AwsClientArgs,
        field_map=_AWS_FIELD_MAP,
    ),
)

S3_DEFAULT_CCP = CcpConfig(
    name="s3-default",
    steps=[],
    mapper=MapperConfig(
        name="aws_client_args",
        schema=AwsClientArgs,
        field_map=_AWS_FIELD_MAP,
    ),
)

MSK_CONNECT_DEFAULT_CCP = CcpConfig(
    name="msk-connect-default",
    steps=[],
    mapper=MapperConfig(
        name="aws_client_args",
        schema=AwsClientArgs,
        field_map=_AWS_FIELD_MAP,
    ),
)

MSK_KAFKA_DEFAULT_CCP = CcpConfig(
    name="msk-kafka-default",
    steps=[],
    mapper=MapperConfig(
        name="aws_client_args",
        schema=AwsClientArgs,
        field_map=_AWS_FIELD_MAP,
    ),
)

# Intentionally not registered — see module docstring above.
# from apollo.integrations.ccp.registry import CcpRegistry  # noqa: E402
# CcpRegistry.register("athena", ATHENA_DEFAULT_CCP)
# CcpRegistry.register("glue", GLUE_DEFAULT_CCP)
# CcpRegistry.register("s3", S3_DEFAULT_CCP)
# CcpRegistry.register("msk-connect", MSK_CONNECT_DEFAULT_CCP)
# CcpRegistry.register("msk-kafka", MSK_KAFKA_DEFAULT_CCP)
