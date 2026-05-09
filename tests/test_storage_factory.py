"""Tests for the storage client factory.

The factory is consumed by `StorageProxyClient` (which previously inlined the
selection logic) and by `HttpProxyClient.download_to_storage`. These tests
exercise the resolution rules without instantiating real cloud SDK clients.
"""

import os
from unittest import TestCase
from unittest.mock import MagicMock, patch

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
from apollo.integrations.storage.factory import get_storage_client

_S3_PATH = "apollo.integrations.s3.s3_reader_writer.S3ReaderWriter"
_GCS_PATH = "apollo.integrations.gcs.gcs_reader_writer.GcsReaderWriter"
_AZURE_PATH = (
    "apollo.integrations.azure_blob.azure_blob_reader_writer.AzureBlobReaderWriter"
)
_S3_COMPAT_PATH = "apollo.integrations.s3_compatible.s3_compatible_reader_writer.S3CompatibleReaderWriter"


class TestGetStorageClient(TestCase):
    def setUp(self) -> None:
        # Strip storage env vars so each test sets exactly what it intends.
        self._saved_env = {
            k: os.environ.pop(k, None)
            for k in (STORAGE_TYPE_ENV_VAR, STORAGE_PREFIX_ENV_VAR)
        }

    def tearDown(self) -> None:
        for k, v in self._saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_env_var_wins_over_platform_default(self):
        os.environ[STORAGE_TYPE_ENV_VAR] = STORAGE_TYPE_S3_COMPATIBLE
        s3_compat_stub = MagicMock(name="s3-compat")
        azure_stub = MagicMock(name="azure")
        with patch(
            _S3_COMPAT_PATH,
            new=s3_compat_stub,
        ), patch(
            _AZURE_PATH,
            new=azure_stub,
        ):
            get_storage_client(platform=PLATFORM_AZURE)  # would default to Azure
        self.assertEqual(1, s3_compat_stub.call_count)
        self.assertEqual(0, azure_stub.call_count)

    def test_platform_default_aws(self):
        s3_stub = MagicMock(name="s3")
        with patch(_S3_PATH, new=s3_stub), patch(_GCS_PATH), patch(_AZURE_PATH):
            get_storage_client(platform=PLATFORM_AWS)
        self.assertEqual(1, s3_stub.call_count)

    def test_platform_default_aws_generic(self):
        s3_stub = MagicMock(name="s3")
        with patch(_S3_PATH, new=s3_stub):
            get_storage_client(platform=PLATFORM_AWS_GENERIC)
        self.assertEqual(1, s3_stub.call_count)

    def test_platform_default_gcp(self):
        gcs_stub = MagicMock(name="gcs")
        with patch(_GCS_PATH, new=gcs_stub), patch(_S3_PATH), patch(_AZURE_PATH):
            get_storage_client(platform=PLATFORM_GCP)
        self.assertEqual(1, gcs_stub.call_count)

    def test_platform_default_azure(self):
        az_stub = MagicMock(name="azure")
        with patch(_AZURE_PATH, new=az_stub), patch(_S3_PATH), patch(_GCS_PATH):
            get_storage_client(platform=PLATFORM_AZURE)
        self.assertEqual(1, az_stub.call_count)

    def test_no_env_no_platform_raises(self):
        with self.assertRaises(AgentConfigurationError) as ctx:
            get_storage_client()
        self.assertIn(STORAGE_TYPE_ENV_VAR, str(ctx.exception))

    def test_unknown_storage_type_raises(self):
        os.environ[STORAGE_TYPE_ENV_VAR] = "unknown-storage-backend"
        with self.assertRaises(AgentConfigurationError) as ctx:
            get_storage_client()
        self.assertIn("unknown-storage-backend", str(ctx.exception))

    def test_prefix_env_var_passed_through(self):
        os.environ[STORAGE_TYPE_ENV_VAR] = STORAGE_TYPE_S3
        os.environ[STORAGE_PREFIX_ENV_VAR] = "my-prefix"
        s3_stub = MagicMock(name="s3")
        with patch(_S3_PATH, new=s3_stub):
            get_storage_client()
        s3_stub.assert_called_once_with(prefix="my-prefix")

    def test_empty_prefix_collapses_to_none(self):
        os.environ[STORAGE_TYPE_ENV_VAR] = STORAGE_TYPE_S3
        os.environ[STORAGE_PREFIX_ENV_VAR] = ""
        s3_stub = MagicMock(name="s3")
        with patch(_S3_PATH, new=s3_stub):
            get_storage_client()
        s3_stub.assert_called_once_with(prefix=None)

    def test_slash_prefix_collapses_to_none(self):
        os.environ[STORAGE_TYPE_ENV_VAR] = STORAGE_TYPE_S3
        os.environ[STORAGE_PREFIX_ENV_VAR] = "/"
        s3_stub = MagicMock(name="s3")
        with patch(_S3_PATH, new=s3_stub):
            get_storage_client()
        s3_stub.assert_called_once_with(prefix=None)

    def test_default_prefix_used_when_env_var_unset(self):
        os.environ[STORAGE_TYPE_ENV_VAR] = STORAGE_TYPE_S3
        # STORAGE_PREFIX_ENV_VAR was stripped in setUp — should default to "mcd".
        s3_stub = MagicMock(name="s3")
        with patch(_S3_PATH, new=s3_stub):
            get_storage_client()
        s3_stub.assert_called_once_with(prefix=STORAGE_PREFIX_DEFAULT_VALUE)
