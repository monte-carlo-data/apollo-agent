from typing import NotRequired, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig


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

ATHENA_DEFAULT_CTP = CtpConfig(
    name="athena-default",
    steps=[],
    mapper=MapperConfig(
        name="aws_client_args",
        schema=AwsClientArgs,
        field_map=_AWS_FIELD_MAP,
    ),
)

GLUE_DEFAULT_CTP = CtpConfig(
    name="glue-default",
    steps=[],
    mapper=MapperConfig(
        name="aws_client_args",
        schema=AwsClientArgs,
        field_map=_AWS_FIELD_MAP,
    ),
)

S3_DEFAULT_CTP = CtpConfig(
    name="s3-default",
    steps=[],
    mapper=MapperConfig(
        name="aws_client_args",
        schema=AwsClientArgs,
        field_map=_AWS_FIELD_MAP,
    ),
)

MSK_CONNECT_DEFAULT_CTP = CtpConfig(
    name="msk-connect-default",
    steps=[],
    mapper=MapperConfig(
        name="aws_client_args",
        schema=AwsClientArgs,
        field_map=_AWS_FIELD_MAP,
    ),
)

MSK_KAFKA_DEFAULT_CTP = CtpConfig(
    name="msk-kafka-default",
    steps=[],
    mapper=MapperConfig(
        name="aws_client_args",
        schema=AwsClientArgs,
        field_map=_AWS_FIELD_MAP,
    ),
)

from apollo.integrations.ctp.registry import CtpRegistry  # noqa: E402

CtpRegistry.register("athena", ATHENA_DEFAULT_CTP)
CtpRegistry.register("glue", GLUE_DEFAULT_CTP)
CtpRegistry.register("s3", S3_DEFAULT_CTP)
CtpRegistry.register("msk-connect", MSK_CONNECT_DEFAULT_CTP)
CtpRegistry.register("msk-kafka", MSK_KAFKA_DEFAULT_CTP)
