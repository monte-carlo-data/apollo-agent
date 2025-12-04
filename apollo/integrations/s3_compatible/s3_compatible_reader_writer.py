import os
from functools import cached_property
from typing import Optional

import boto3
from botocore.config import Config

from apollo.agent.env_vars import (
    STORAGE_BUCKET_NAME_ENV_VAR,
    STORAGE_ENDPOINT_URL_ENV_VAR,
    STORAGE_ACCESS_KEY_ENV_VAR,
    STORAGE_SECRET_KEY_ENV_VAR,
)
from apollo.agent.models import AgentConfigurationError
from apollo.integrations.s3.s3_base_reader_writer import S3BaseReaderWriter


class S3CompatibleReaderWriter(S3BaseReaderWriter):
    """
    The implementation of S3-compatible storage client used in the agent. This class
    supports any S3-compatible storage backend (MinIO, Ceph, DigitalOcean Spaces, etc.)
    by extending S3BaseReaderWriter and configuring boto3 clients with a custom endpoint
    URL and credentials from environment variables.
    """

    def __init__(self, prefix: Optional[str] = None):
        bucket_name = os.getenv(STORAGE_BUCKET_NAME_ENV_VAR)
        if not bucket_name:
            raise AgentConfigurationError(
                f"Bucket not configured, {STORAGE_BUCKET_NAME_ENV_VAR} env var expected"
            )

        endpoint_url = os.getenv(STORAGE_ENDPOINT_URL_ENV_VAR)
        if not endpoint_url:
            raise AgentConfigurationError(
                f"S3-compatible storage endpoint not configured, {STORAGE_ENDPOINT_URL_ENV_VAR} env var expected"
            )

        access_key = os.getenv(STORAGE_ACCESS_KEY_ENV_VAR)
        if not access_key:
            raise AgentConfigurationError(
                f"S3-compatible storage access key not configured, {STORAGE_ACCESS_KEY_ENV_VAR} env var expected"
            )

        secret_key = os.getenv(STORAGE_SECRET_KEY_ENV_VAR)
        if not secret_key:
            raise AgentConfigurationError(
                f"S3-compatible storage secret key not configured, {STORAGE_SECRET_KEY_ENV_VAR} env var expected"
            )

        self._endpoint_url = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key

        super().__init__(bucket_name=bucket_name, prefix=prefix)

    @cached_property
    def s3_client(self):
        """
        Creates a new S3-compatible client configured with the custom endpoint URL
        and credentials from environment variables.
        """
        return boto3.client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        )

    @cached_property
    def s3_regional_client(self):
        """
        Creates a new S3-compatible client initialized with the custom endpoint URL,
        required for pre-signed urls. S3-compatible storage uses the same endpoint for all operations.
        """
        config = Config(signature_version="s3v4")
        return boto3.client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            config=config,
        )

    @cached_property
    def s3_resource(self):
        """
        Creates a new S3-compatible resource configured with the custom endpoint URL
        and credentials from environment variables.
        """
        return boto3.resource(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        )

    def _get_s3_client_with_config(self, config: Config):
        """
        Returns a client for S3-compatible storage with the provided configuration, used to create clients
        with a different connect_timeout and max_attempts when testing connectivity to
        S3-compatible storage endpoints.
        """
        return boto3.client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            config=config,
        )
