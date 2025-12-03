import os
from functools import cached_property
from typing import Optional

import boto3
from botocore.config import Config
from botocore.client import BaseClient

from apollo.agent.env_vars import (
    STORAGE_BUCKET_NAME_ENV_VAR,
    MINIO_ENDPOINT_URL_ENV_VAR,
    MINIO_ACCESS_KEY_ENV_VAR,
    MINIO_SECRET_KEY_ENV_VAR,
)
from apollo.agent.models import AgentConfigurationError
from apollo.integrations.s3.s3_base_reader_writer import S3BaseReaderWriter


class MinIOReaderWriter(S3BaseReaderWriter):
    """
    The implementation of MinIO storage client used in the agent. MinIO is S3-compatible,
    so this class extends S3BaseReaderWriter and configures boto3 clients with the MinIO
    endpoint URL and credentials from environment variables.
    """

    def __init__(self, prefix: Optional[str] = None):
        bucket_name = os.getenv(STORAGE_BUCKET_NAME_ENV_VAR)
        if not bucket_name:
            raise AgentConfigurationError(
                f"Bucket not configured, {STORAGE_BUCKET_NAME_ENV_VAR} env var expected"
            )

        endpoint_url = os.getenv(MINIO_ENDPOINT_URL_ENV_VAR)
        if not endpoint_url:
            raise AgentConfigurationError(
                f"MinIO endpoint not configured, {MINIO_ENDPOINT_URL_ENV_VAR} env var expected"
            )

        access_key = os.getenv(MINIO_ACCESS_KEY_ENV_VAR)
        if not access_key:
            raise AgentConfigurationError(
                f"MinIO access key not configured, {MINIO_ACCESS_KEY_ENV_VAR} env var expected"
            )

        secret_key = os.getenv(MINIO_SECRET_KEY_ENV_VAR)
        if not secret_key:
            raise AgentConfigurationError(
                f"MinIO secret key not configured, {MINIO_SECRET_KEY_ENV_VAR} env var expected"
            )

        self._endpoint_url = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key

        super().__init__(bucket_name=bucket_name, prefix=prefix)

    @cached_property
    def s3_client(self):
        """
        Creates a new S3-compatible client configured for MinIO with the endpoint URL
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
        Creates a new S3-compatible client initialized with the MinIO endpoint URL,
        required for pre-signed urls. MinIO uses the same endpoint for all operations.
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
        Creates a new S3-compatible resource configured for MinIO with the endpoint URL
        and credentials from environment variables.
        """
        return boto3.resource(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        )

    def _get_s3_client_with_config(self, config: Config) -> BaseClient:
        """
        Returns a client for MinIO with the provided configuration, used to create clients
        with a different connect_timeout and max_attempts when testing connectivity to
        MinIO endpoints.
        """
        return boto3.client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            config=config,
        )
