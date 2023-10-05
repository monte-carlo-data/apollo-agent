import os
from functools import cached_property

import boto3

from apollo.integrations.s3.s3_base_reader_writer import S3BaseReaderWriter

CONFIGURATION_BUCKET = os.getenv("CONFIGURATION_BUCKET", "data-collector-configuration")


class S3ReaderWriter(S3BaseReaderWriter):
    def __init__(self):
        super().__init__(CONFIGURATION_BUCKET)

    @cached_property
    def s3_client(self):
        return boto3.client("s3")

    @cached_property
    def s3_resource(self):
        return boto3.resource("s3")
