"""Instantiate the configured BaseStorageClient.

Factored out of `StorageProxyClient` so other proxy clients (e.g.
`HttpProxyClient.download_to_storage`) can grab the configured storage
backend without constructing a full `StorageProxyClient`.
"""

import os
from typing import Optional, cast

from apollo.common.agent.constants import (
    PLATFORM_AWS,
    PLATFORM_AWS_GENERIC,
    PLATFORM_AZURE,
    PLATFORM_GCP,
    STORAGE_TYPE_AZURE,
    STORAGE_TYPE_GCS,
    STORAGE_TYPE_S3,
    STORAGE_TYPE_S3_COMPATIBLE,
)
from apollo.common.agent.env_vars import (
    STORAGE_PREFIX_DEFAULT_VALUE,
    STORAGE_PREFIX_ENV_VAR,
    STORAGE_TYPE_ENV_VAR,
)
from apollo.common.agent.models import AgentConfigurationError
from apollo.integrations.azure_blob.azure_blob_reader_writer import (
    AzureBlobReaderWriter,
)
from apollo.integrations.gcs.gcs_reader_writer import GcsReaderWriter
from apollo.integrations.s3.s3_reader_writer import S3ReaderWriter
from apollo.integrations.s3_compatible.s3_compatible_reader_writer import (
    S3CompatibleReaderWriter,
)
from apollo.integrations.storage.base_storage_client import BaseStorageClient

_DEFAULT_PLATFORM_STORAGE = {
    PLATFORM_AZURE: STORAGE_TYPE_AZURE,
    PLATFORM_GCP: STORAGE_TYPE_GCS,
    PLATFORM_AWS: STORAGE_TYPE_S3,
    PLATFORM_AWS_GENERIC: STORAGE_TYPE_S3,
}

_STORAGE_CLIENTS = {
    STORAGE_TYPE_AZURE: AzureBlobReaderWriter,
    STORAGE_TYPE_GCS: GcsReaderWriter,
    STORAGE_TYPE_S3: S3ReaderWriter,
    STORAGE_TYPE_S3_COMPATIBLE: S3CompatibleReaderWriter,
}


def get_storage_client(platform: Optional[str] = None) -> BaseStorageClient:
    """Return the configured `BaseStorageClient` instance.

    Resolution order for the storage backend:
      1. `MCD_STORAGE` env var (highest priority — explicit override)
      2. Platform default (`PLATFORM_AWS` → S3, `PLATFORM_GCP` → GCS,
         `PLATFORM_AZURE` → Azure). Used only when `platform` is provided.

    Prefix is read from `MCD_STORAGE_PREFIX`; an empty / `/` value collapses
    to no prefix.

    Raises `AgentConfigurationError` if the storage type cannot be resolved
    or names an unknown backend.
    """
    storage = os.getenv(STORAGE_TYPE_ENV_VAR)
    if not storage and platform:
        storage = _DEFAULT_PLATFORM_STORAGE.get(platform)
    if not storage:
        raise AgentConfigurationError(
            f"Missing {STORAGE_TYPE_ENV_VAR} env var and no platform default available"
        )

    storage_class = _STORAGE_CLIENTS.get(storage)
    if not storage_class:
        raise AgentConfigurationError(f"Invalid storage type: {storage}")

    prefix: Optional[str] = os.getenv(
        STORAGE_PREFIX_ENV_VAR, STORAGE_PREFIX_DEFAULT_VALUE
    )
    if prefix in ("", "/"):
        prefix = None

    return cast(BaseStorageClient, storage_class(prefix=prefix))
