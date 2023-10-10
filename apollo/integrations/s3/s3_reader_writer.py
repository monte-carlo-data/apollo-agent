import os
from functools import cached_property

import boto3

from apollo.integrations.s3.s3_base_reader_writer import S3BaseReaderWriter

CONFIGURATION_BUCKET = os.getenv("CONFIGURATION_BUCKET", "data-collector-configuration")


class S3ReaderWriter(S3BaseReaderWriter):
    """
    The implementation of S3 storage client used in the agent, the S3 client and resource are created using the
    default settings supported by boto3 library, which means env vars need to be set with the credentials to use.
    """

    def __init__(self):
        super().__init__(CONFIGURATION_BUCKET)

    @cached_property
    def s3_client(self):
        return boto3.client("s3")

    @cached_property
    def s3_resource(self):
        return boto3.resource("s3")
