import os
from functools import cached_property
from typing import Optional

import boto3

from apollo.agent.env_vars import STORAGE_BUCKET_NAME_ENV_VAR
from apollo.agent.models import AgentConfigurationError
from apollo.integrations.s3.s3_base_reader_writer import S3BaseReaderWriter


class S3ReaderWriter(S3BaseReaderWriter):
    """
    The implementation of S3 storage client used in the agent, the S3 client and resource are created using the
    default settings supported by boto3 library, which means env vars need to be set with the credentials to use.
    """

    def __init__(self, prefix: Optional[str] = None):
        bucket_name = os.getenv(STORAGE_BUCKET_NAME_ENV_VAR)
        if not bucket_name:
            raise AgentConfigurationError(
                f"Bucket not configured, {STORAGE_BUCKET_NAME_ENV_VAR} env var expected"
            )
        super().__init__(bucket_name=bucket_name, prefix=prefix)

    @cached_property
    def s3_client(self):
        """
        Creates a new S3 client with the default settings and using credentials from the environment
        """
        return boto3.client("s3")

    @cached_property
    def s3_regional_client(self):
        """
        Creates a new S3 client initialized with the regional endpoint and
        using credentials from the environment, required for pre-signed urls,
        see: https://github.com/boto/boto3/issues/3015
        """
        return boto3.client("s3", endpoint_url=self.s3_client.meta.endpoint_url)

    @cached_property
    def s3_resource(self):
        """
        Creates a new S3 resource with the default settings and using credentials
        from the environment.
        """
        return boto3.resource("s3")
