import datetime
import os
from unittest import TestCase
from unittest.mock import (
    MagicMock,
    patch,
    create_autospec,
    mock_open,
)

from azure.storage.blob import (
    BlobServiceClient,
    ContainerClient,
    BlobClient,
)
from box import Box

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    PLATFORM_AZURE,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR,
)
from apollo.common.agent.env_vars import (
    STORAGE_BUCKET_NAME_ENV_VAR,
    STORAGE_PREFIX_ENV_VAR,
    STORAGE_PREFIX_DEFAULT_VALUE,
    STORAGE_ACCOUNT_NAME_ENV_VAR,
)
from apollo.common.agent.models import AgentConfigurationError
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.utils import AgentUtils
from apollo.integrations.azure_blob.azure_blob_reader_writer import (
    AzureBlobReaderWriter,
    AZURE_STORAGE_AUTH_TYPE_ENV_VAR,
    AZURE_SP_TENANT_ID_ENV_VAR,
    AZURE_SP_CLIENT_ID_ENV_VAR,
    AZURE_SP_CLIENT_SECRET_ENV_VAR,
    AZURE_STORAGE_ACCOUNT_URL_ENV_VAR,
    AUTH_TYPE_AZURE_SERVICE_PRINCIPAL,
)
from tests.platform_provider import TestPlatformProvider

_TEST_BUCKET_NAME = "test_bucket"
_TEST_ACCOUNT_NAME = "test_account"
_TEST_ENVIRON = {
    STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
    STORAGE_ACCOUNT_NAME_ENV_VAR: _TEST_ACCOUNT_NAME,
}
_TEST_ENVIRON_EMPTY_PREFIX = {
    **_TEST_ENVIRON,
    STORAGE_PREFIX_ENV_VAR: "",
}

_TEST_TENANT_ID = "test-tenant-id"
_TEST_CLIENT_ID = "test-client-id"
_TEST_CLIENT_SECRET = "test-client-secret"
_TEST_CUSTOM_ACCOUNT_URL = "https://myaccount.privatelink.blob.core.windows.net"
_TEST_SP_ENVIRON = {
    **_TEST_ENVIRON_EMPTY_PREFIX,
    AZURE_STORAGE_AUTH_TYPE_ENV_VAR: AUTH_TYPE_AZURE_SERVICE_PRINCIPAL,
    AZURE_SP_TENANT_ID_ENV_VAR: _TEST_TENANT_ID,
    AZURE_SP_CLIENT_ID_ENV_VAR: _TEST_CLIENT_ID,
    AZURE_SP_CLIENT_SECRET_ENV_VAR: _TEST_CLIENT_SECRET,
}


class StorageAzureTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._agent.platform_provider = TestPlatformProvider(PLATFORM_AZURE)

        self._mock_service_client = create_autospec(BlobServiceClient)
        self._mock_container_client = create_autospec(ContainerClient)
        self._mock_blob_client = create_autospec(BlobClient)

    @patch.dict(
        os.environ,
        _TEST_ENVIRON_EMPTY_PREFIX,
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    def test_list_objects(self, mock_client_type):
        self._mock_service_client.get_container_client.return_value = (
            self._mock_container_client
        )
        mock_client_type.return_value = self._mock_service_client
        page = [
            Box(
                etag="123",
                name="file_1.txt",
                size=23,
                last_modified=datetime.datetime.utcnow(),
                blob_tier="Hot",
            )
        ]
        pages_result = MagicMock()
        pages_result.__next__.return_value = page
        pages_result.continuation_token = "12345"
        list_blobs_result = MagicMock()
        list_blobs_result.by_page.return_value = pages_result
        self._mock_container_client.list_blobs.return_value = list_blobs_result

        result = self._agent.execute_operation(
            "storage",
            "list_objects",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [{"method": "list_objects"}],
            },
            credentials={},
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))
        self._mock_container_client.list_blobs.assert_called_with()

        response = result.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual("12345", response["page_token"])
        self.assertEqual(page[0].name, response["list"][0]["Key"])
        self.assertEqual(page[0].size, response["list"][0]["Size"])
        self.assertEqual(page[0].etag, response["list"][0]["ETag"])
        self.assertEqual(page[0].last_modified, response["list"][0]["LastModified"])
        self.assertEqual(page[0].blob_tier, response["list"][0]["StorageClass"])

    @patch.dict(
        os.environ,
        _TEST_ENVIRON,
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    def test_list_objects_default_prefix(self, mock_client_type):
        self._mock_service_client.get_container_client.return_value = (
            self._mock_container_client
        )
        mock_client_type.return_value = self._mock_service_client
        expected_prefix = f"{STORAGE_PREFIX_DEFAULT_VALUE}/"
        file_name = "file_1.txt"
        page = [
            Box(
                etag="123",
                name=f"{expected_prefix}{file_name}",
                size=23,
                last_modified=datetime.datetime.utcnow(),
                blob_tier="Hot",
            )
        ]
        pages_result = MagicMock()
        pages_result.__next__.return_value = page
        pages_result.continuation_token = "12345"
        list_blobs_result = MagicMock()
        list_blobs_result.by_page.return_value = pages_result
        self._mock_container_client.list_blobs.return_value = list_blobs_result

        result = self._agent.execute_operation(
            "storage",
            "list_objects",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [{"method": "list_objects"}],
            },
            credentials={},
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        self._mock_container_client.list_blobs.assert_called_with(
            name_starts_with=expected_prefix,
        )

        response = result.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual("12345", response["page_token"])
        self.assertEqual(file_name, response["list"][0]["Key"])
        self.assertEqual(page[0].size, response["list"][0]["Size"])
        self.assertEqual(page[0].etag, response["list"][0]["ETag"])
        self.assertEqual(page[0].last_modified, response["list"][0]["LastModified"])
        self.assertEqual(page[0].blob_tier, response["list"][0]["StorageClass"])

    @patch.dict(
        os.environ,
        _TEST_ENVIRON_EMPTY_PREFIX,
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    def test_delete(self, mock_client_type):
        self._mock_service_client.get_blob_client.return_value = self._mock_blob_client
        mock_client_type.return_value = self._mock_service_client

        file_key = "file.txt"
        result = self._agent.execute_operation(
            "storage",
            "delete",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [{"method": "delete", "kwargs": {"key": file_key}}],
            },
            credentials={},
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        self._mock_service_client.get_blob_client.assert_called_once_with(
            container=_TEST_BUCKET_NAME, blob=file_key
        )
        self._mock_blob_client.delete_blob.assert_called_once_with()

    @patch.dict(os.environ, _TEST_ENVIRON)
    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    def test_delete_default_prefix(self, mock_client_type):
        self._mock_service_client.get_blob_client.return_value = self._mock_blob_client
        mock_client_type.return_value = self._mock_service_client
        expected_prefix = f"{STORAGE_PREFIX_DEFAULT_VALUE}/"

        file_key = "file.txt"
        result = self._agent.execute_operation(
            "storage",
            "delete",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [{"method": "delete", "kwargs": {"key": file_key}}],
            },
            credentials={},
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        self._mock_service_client.get_blob_client.assert_called_once_with(
            container=_TEST_BUCKET_NAME, blob=f"{expected_prefix}{file_key}"
        )
        self._mock_blob_client.delete_blob.assert_called_once_with()

    @patch.dict(
        os.environ,
        _TEST_ENVIRON_EMPTY_PREFIX,
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    def test_read(self, mock_client_type):
        self._mock_service_client.get_blob_client.return_value = self._mock_blob_client
        mock_client_type.return_value = self._mock_service_client

        file_key = "file.txt"
        result = self._agent.execute_operation(
            "storage",
            "read",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [{"method": "read", "kwargs": {"key": file_key}}],
            },
            credentials={},
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        self._mock_service_client.get_blob_client.assert_called_once_with(
            container=_TEST_BUCKET_NAME, blob=file_key
        )
        self._mock_blob_client.download_blob.assert_called_once_with()

    @patch.dict(
        os.environ,
        _TEST_ENVIRON,
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    def test_read_default_prefix(self, mock_client_type):
        self._mock_service_client.get_blob_client.return_value = self._mock_blob_client
        mock_client_type.return_value = self._mock_service_client
        expected_prefix = f"{STORAGE_PREFIX_DEFAULT_VALUE}/"

        file_key = "file.txt"
        result = self._agent.execute_operation(
            "storage",
            "read",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [{"method": "read", "kwargs": {"key": file_key}}],
            },
            credentials={},
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        self._mock_service_client.get_blob_client.assert_called_once_with(
            container=_TEST_BUCKET_NAME, blob=f"{expected_prefix}{file_key}"
        )
        self._mock_blob_client.download_blob.assert_called_once_with()

    @patch.dict(
        os.environ,
        _TEST_ENVIRON_EMPTY_PREFIX,
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    @patch.object(AgentUtils, "temp_file_path")
    def test_download(self, mock_temp_file_path, mock_client_type):
        tmp_path = "/tmp/temp.data"
        mock_temp_file_path.return_value = tmp_path
        self._mock_service_client.get_blob_client.return_value = self._mock_blob_client
        mock_client_type.return_value = self._mock_service_client

        file_key = "file.txt"
        with patch("builtins.open", mock_open()):
            result = self._agent.execute_operation(
                "storage",
                "download_file",
                {
                    "trace_id": "1234",
                    "skip_cache": True,
                    "commands": [
                        {"method": "download_file", "kwargs": {"key": file_key}}
                    ],
                },
                credentials={},
            )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        self._mock_service_client.get_blob_client.assert_called_once_with(
            container=_TEST_BUCKET_NAME, blob=file_key
        )
        self._mock_blob_client.download_blob.assert_called_once_with()
        mock_temp_file_path.assert_called_once()

    @patch.dict(
        os.environ,
        _TEST_ENVIRON,
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    @patch.object(AgentUtils, "temp_file_path")
    def test_download_default_prefix(self, mock_temp_file_path, mock_client_type):
        tmp_path = "/tmp/temp.data"
        mock_temp_file_path.return_value = tmp_path
        self._mock_service_client.get_blob_client.return_value = self._mock_blob_client
        mock_client_type.return_value = self._mock_service_client
        expected_prefix = f"{STORAGE_PREFIX_DEFAULT_VALUE}/"

        file_key = "file.txt"
        with patch("builtins.open", mock_open()):
            result = self._agent.execute_operation(
                "storage",
                "download_file",
                {
                    "trace_id": "1234",
                    "skip_cache": True,
                    "commands": [
                        {"method": "download_file", "kwargs": {"key": file_key}}
                    ],
                },
                credentials={},
            )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        self._mock_service_client.get_blob_client.assert_called_once_with(
            container=_TEST_BUCKET_NAME, blob=f"{expected_prefix}{file_key}"
        )
        self._mock_blob_client.download_blob.assert_called_once_with()
        mock_temp_file_path.assert_called_once()


class ServicePrincipalAuthTests(TestCase):
    """Tests for service principal (OAuth) authentication in AzureBlobReaderWriter."""

    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_reader_writer.ClientSecretCredential"
    )
    @patch.dict(os.environ, _TEST_SP_ENVIRON)
    def test_sp_init_constructs_client_secret_credential(
        self, mock_credential_cls, mock_blob_client_cls
    ):
        mock_credential = MagicMock()
        mock_credential_cls.return_value = mock_credential

        writer = AzureBlobReaderWriter()

        mock_credential_cls.assert_called_once_with(
            tenant_id=_TEST_TENANT_ID,
            client_id=_TEST_CLIENT_ID,
            client_secret=_TEST_CLIENT_SECRET,
        )
        mock_blob_client_cls.assert_called_once_with(
            f"https://{_TEST_ACCOUNT_NAME}.blob.core.windows.net",
            mock_credential,
        )

    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_reader_writer.ClientSecretCredential"
    )
    @patch.dict(os.environ, _TEST_SP_ENVIRON)
    def test_sp_does_not_call_default_credential(
        self, mock_credential_cls, mock_blob_client_cls
    ):
        with patch(
            "apollo.integrations.azure_blob.azure_blob_reader_writer.AzureUtils.get_default_credential"
        ) as mock_default_cred:
            AzureBlobReaderWriter()
            mock_default_cred.assert_not_called()

    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_reader_writer.ClientSecretCredential"
    )
    @patch.dict(
        os.environ,
        {
            **_TEST_SP_ENVIRON,
            AZURE_STORAGE_ACCOUNT_URL_ENV_VAR: _TEST_CUSTOM_ACCOUNT_URL,
        },
    )
    def test_sp_uses_custom_account_url(
        self, mock_credential_cls, mock_blob_client_cls
    ):
        mock_credential = MagicMock()
        mock_credential_cls.return_value = mock_credential

        AzureBlobReaderWriter()

        mock_blob_client_cls.assert_called_once_with(
            _TEST_CUSTOM_ACCOUNT_URL,
            mock_credential,
        )

    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_reader_writer.ClientSecretCredential"
    )
    @patch.dict(os.environ, _TEST_SP_ENVIRON)
    def test_sp_falls_back_to_constructed_url_when_no_custom_url(
        self, mock_credential_cls, mock_blob_client_cls
    ):
        mock_credential = MagicMock()
        mock_credential_cls.return_value = mock_credential

        AzureBlobReaderWriter()

        expected_url = f"https://{_TEST_ACCOUNT_NAME}.blob.core.windows.net"
        mock_blob_client_cls.assert_called_once_with(expected_url, mock_credential)

    @patch(
        "apollo.integrations.azure_blob.azure_blob_reader_writer.ClientSecretCredential"
    )
    @patch.dict(
        os.environ,
        {
            **_TEST_SP_ENVIRON,
            AZURE_STORAGE_ACCOUNT_URL_ENV_VAR: "http://insecure.blob.core.windows.net",
        },
    )
    def test_sp_rejects_non_https_account_url(self, mock_credential_cls):
        with self.assertRaises(AgentConfigurationError) as ctx:
            AzureBlobReaderWriter()
        self.assertIn("https://", str(ctx.exception))

    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_reader_writer.ClientSecretCredential"
    )
    @patch.dict(os.environ, _TEST_SP_ENVIRON)
    def test_sp_is_bucket_private_returns_true(
        self, mock_credential_cls, mock_blob_client_cls
    ):
        mock_service = create_autospec(BlobServiceClient)
        mock_blob_client_cls.return_value = mock_service

        writer = AzureBlobReaderWriter()
        result = writer.is_bucket_private()

        self.assertTrue(result)
        # Should not attempt to get container access policy
        mock_service.get_container_client.return_value.get_container_access_policy.assert_not_called()

    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_reader_writer.ClientSecretCredential"
    )
    @patch.dict(os.environ, _TEST_SP_ENVIRON)
    def test_sp_generate_presigned_url_uses_delegation_key(
        self, mock_credential_cls, mock_blob_client_cls
    ):
        mock_service = create_autospec(BlobServiceClient)
        mock_blob_client_cls.return_value = mock_service
        mock_blob = create_autospec(BlobClient)
        mock_blob.container_name = _TEST_BUCKET_NAME
        mock_blob.blob_name = "test.txt"
        mock_blob.url = f"https://{_TEST_ACCOUNT_NAME}.blob.core.windows.net/{_TEST_BUCKET_NAME}/test.txt"
        mock_service.get_blob_client.return_value = mock_blob

        writer = AzureBlobReaderWriter()

        with patch(
            "apollo.integrations.azure_blob.azure_blob_reader_writer.generate_blob_sas",
            return_value="sas_token",
        ) as mock_generate_sas:
            writer.generate_presigned_url("test.txt", datetime.timedelta(hours=1))

            mock_service.get_user_delegation_key.assert_called_once()
            mock_generate_sas.assert_called_once()
            call_kwargs = mock_generate_sas.call_args
            self.assertEqual(call_kwargs.kwargs.get("account_name"), _TEST_ACCOUNT_NAME)
            self.assertIn("user_delegation_key", call_kwargs.kwargs)
            self.assertNotIn("account_key", call_kwargs.kwargs)

    @patch(
        "apollo.integrations.azure_blob.azure_blob_reader_writer.ClientSecretCredential"
    )
    def test_sp_missing_tenant_id_raises(self, mock_credential_cls):
        env = {**_TEST_SP_ENVIRON}
        del env[AZURE_SP_TENANT_ID_ENV_VAR]
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(AgentConfigurationError) as ctx:
                AzureBlobReaderWriter()
            self.assertIn(AZURE_SP_TENANT_ID_ENV_VAR, str(ctx.exception))

    @patch(
        "apollo.integrations.azure_blob.azure_blob_reader_writer.ClientSecretCredential"
    )
    def test_sp_missing_client_id_raises(self, mock_credential_cls):
        env = {**_TEST_SP_ENVIRON}
        del env[AZURE_SP_CLIENT_ID_ENV_VAR]
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(AgentConfigurationError) as ctx:
                AzureBlobReaderWriter()
            self.assertIn(AZURE_SP_CLIENT_ID_ENV_VAR, str(ctx.exception))

    @patch(
        "apollo.integrations.azure_blob.azure_blob_reader_writer.ClientSecretCredential"
    )
    def test_sp_missing_client_secret_raises(self, mock_credential_cls):
        env = {**_TEST_SP_ENVIRON}
        del env[AZURE_SP_CLIENT_SECRET_ENV_VAR]
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(AgentConfigurationError) as ctx:
                AzureBlobReaderWriter()
            self.assertIn(AZURE_SP_CLIENT_SECRET_ENV_VAR, str(ctx.exception))

    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    @patch.dict(os.environ, _TEST_ENVIRON_EMPTY_PREFIX)
    def test_managed_identity_path_unchanged_with_unset_auth_type(
        self, mock_blob_client_cls
    ):
        with patch(
            "apollo.integrations.azure_blob.azure_blob_reader_writer.AzureUtils.get_default_credential"
        ) as mock_default_cred:
            mock_credential = MagicMock()
            mock_default_cred.return_value = mock_credential

            writer = AzureBlobReaderWriter()

            mock_default_cred.assert_called_once()
            expected_url = f"https://{_TEST_ACCOUNT_NAME}.blob.core.windows.net"
            mock_blob_client_cls.assert_called_once_with(expected_url, mock_credential)

    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_reader_writer.ClientSecretCredential"
    )
    @patch.dict(os.environ, _TEST_SP_ENVIRON)
    def test_sp_through_agent_execute_operation(
        self, mock_credential_cls, mock_blob_client_cls
    ):
        """Verify SP path works through the full Agent.execute_operation → factory wiring."""
        agent = Agent(LoggingUtils())
        agent.platform_provider = TestPlatformProvider(PLATFORM_AZURE)

        mock_service = create_autospec(BlobServiceClient)
        mock_blob_client_cls.return_value = mock_service
        mock_blob = create_autospec(BlobClient)
        mock_service.get_blob_client.return_value = mock_blob

        result = agent.execute_operation(
            "storage",
            "read",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [{"method": "read", "kwargs": {"key": "test.txt"}}],
            },
            credentials={},
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))
        mock_credential_cls.assert_called_once_with(
            tenant_id=_TEST_TENANT_ID,
            client_id=_TEST_CLIENT_ID,
            client_secret=_TEST_CLIENT_SECRET,
        )
