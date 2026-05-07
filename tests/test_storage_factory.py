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
    PLATFORM_AZURE,
    PLATFORM_GCP,
    STORAGE_TYPE_AZURE,
    STORAGE_TYPE_GCS,
    STORAGE_TYPE_S3,
    STORAGE_TYPE_S3_COMPATIBLE,
)
from apollo.common.agent.env_vars import (
    STORAGE_PREFIX_ENV_VAR,
    STORAGE_TYPE_ENV_VAR,
)
from apollo.common.agent.models import AgentConfigurationError
from apollo.integrations.storage import factory as factory_module
from apollo.integrations.storage.factory import get_storage_client


def _patched_clients(stub: MagicMock) -> dict:
    """Map every storage type to the same MagicMock — so tests can assert which
    type was selected by inspecting the kwargs the mock was called with."""
    return {
        STORAGE_TYPE_AZURE: stub,
        STORAGE_TYPE_GCS: stub,
        STORAGE_TYPE_S3: stub,
        STORAGE_TYPE_S3_COMPATIBLE: stub,
    }


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

    def _patch_clients_with_stub(self):
        stub = MagicMock(name="storage_client_class")
        return stub, patch.dict(
            factory_module._STORAGE_CLIENTS, _patched_clients(stub), clear=True
        )

    def test_env_var_wins_over_platform_default(self):
        os.environ[STORAGE_TYPE_ENV_VAR] = STORAGE_TYPE_S3_COMPATIBLE
        stub, ctx = self._patch_clients_with_stub()
        with ctx:
            get_storage_client(platform=PLATFORM_AZURE)  # would default to azure
        # The env var pin (S3_COMPATIBLE) wins over the platform default (Azure).
        # All four classes are the same stub, so we assert it was called once,
        # not which class.
        self.assertEqual(1, stub.call_count)

    def test_platform_default_aws(self):
        stub, ctx = self._patch_clients_with_stub()
        with patch.dict(
            factory_module._STORAGE_CLIENTS,
            {
                STORAGE_TYPE_S3: stub,
                STORAGE_TYPE_GCS: MagicMock(),
                STORAGE_TYPE_AZURE: MagicMock(),
            },
            clear=True,
        ):
            get_storage_client(platform=PLATFORM_AWS)
        self.assertEqual(1, stub.call_count)

    def test_platform_default_gcp(self):
        gcs_stub = MagicMock(name="gcs")
        with patch.dict(
            factory_module._STORAGE_CLIENTS,
            {
                STORAGE_TYPE_GCS: gcs_stub,
                STORAGE_TYPE_S3: MagicMock(),
                STORAGE_TYPE_AZURE: MagicMock(),
            },
            clear=True,
        ):
            get_storage_client(platform=PLATFORM_GCP)
        self.assertEqual(1, gcs_stub.call_count)

    def test_platform_default_azure(self):
        az_stub = MagicMock(name="azure")
        with patch.dict(
            factory_module._STORAGE_CLIENTS,
            {
                STORAGE_TYPE_AZURE: az_stub,
                STORAGE_TYPE_S3: MagicMock(),
                STORAGE_TYPE_GCS: MagicMock(),
            },
            clear=True,
        ):
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
        stub, ctx = self._patch_clients_with_stub()
        with ctx:
            get_storage_client()
        stub.assert_called_once_with(prefix="my-prefix")

    def test_empty_prefix_collapses_to_none(self):
        os.environ[STORAGE_TYPE_ENV_VAR] = STORAGE_TYPE_S3
        os.environ[STORAGE_PREFIX_ENV_VAR] = ""
        stub, ctx = self._patch_clients_with_stub()
        with ctx:
            get_storage_client()
        stub.assert_called_once_with(prefix=None)

    def test_slash_prefix_collapses_to_none(self):
        os.environ[STORAGE_TYPE_ENV_VAR] = STORAGE_TYPE_S3
        os.environ[STORAGE_PREFIX_ENV_VAR] = "/"
        stub, ctx = self._patch_clients_with_stub()
        with ctx:
            get_storage_client()
        stub.assert_called_once_with(prefix=None)
