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
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.utils import AgentUtils
from apollo.common.agent.models import AgentConfigurationError
from apollo.integrations.azure_blob.azure_blob_reader_writer import (
    AzureBlobReaderWriter,
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


_TEST_ACCOUNT_KEY = "dGVzdC1hY2NvdW50LWtleQ=="
_TEST_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=https;"
    f"AccountName={_TEST_ACCOUNT_NAME};"
    f"AccountKey={_TEST_ACCOUNT_KEY};"
    "EndpointSuffix=core.windows.net"
)
# Spelled out rather than imported from the integration, so that renaming the env var (a
# documented, customer-facing name) fails here.
_CONNECTION_STRING_ENV_VAR = "MCD_STORAGE_CONNECTION_STRING"
_TEST_ENVIRON_CONNECTION_STRING = {
    STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
    _CONNECTION_STRING_ENV_VAR: _TEST_CONNECTION_STRING,
}


class StorageAzureConnectionStringTests(TestCase):
    """
    Connection-string authentication, used when the agent has no access to Entra. The account name
    and shared key come from the string, so the Entra-only code paths in `AzureBlobReaderWriter`
    must all defer to the base class.
    """

    @patch.dict(os.environ, _TEST_ENVIRON_CONNECTION_STRING, clear=True)
    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    def test_client_is_built_from_the_connection_string(self, mock_client_type):
        AzureBlobReaderWriter()

        mock_client_type.from_connection_string.assert_called_once_with(
            conn_str=_TEST_CONNECTION_STRING
        )
        # The base class checks account_url/credential first, so passing those alongside a
        # connection string would silently ignore the string.
        mock_client_type.assert_not_called()

    @patch.dict(
        os.environ,
        {**_TEST_ENVIRON_CONNECTION_STRING, STORAGE_ACCOUNT_NAME_ENV_VAR: "ignored"},
        clear=True,
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    def test_connection_string_takes_precedence_over_the_account_name(
        self, mock_client_type
    ):
        AzureBlobReaderWriter()

        mock_client_type.from_connection_string.assert_called_once_with(
            conn_str=_TEST_CONNECTION_STRING
        )
        mock_client_type.assert_not_called()

    @patch.dict(
        os.environ, {STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME}, clear=True
    )
    def test_neither_connection_string_nor_account_name_is_a_configuration_error(self):
        with self.assertRaises(AgentConfigurationError) as context:
            AzureBlobReaderWriter()
        self.assertIn(STORAGE_ACCOUNT_NAME_ENV_VAR, str(context.exception))

    @patch.dict(os.environ, _TEST_ENVIRON_CONNECTION_STRING, clear=True)
    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.generate_blob_sas"
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    def test_presigned_url_is_signed_with_the_account_key(
        self, mock_client_type, mock_generate_blob_sas
    ):
        service_client = mock_client_type.from_connection_string.return_value
        blob_client = service_client.get_blob_client.return_value
        blob_client.credential.account_name = _TEST_ACCOUNT_NAME
        blob_client.credential.account_key = _TEST_ACCOUNT_KEY
        blob_client.url = (
            f"https://{_TEST_ACCOUNT_NAME}.blob.core.windows.net/"
            f"{_TEST_BUCKET_NAME}/{STORAGE_PREFIX_DEFAULT_VALUE}/file.txt"
        )
        mock_generate_blob_sas.return_value = "sig=abc"

        url = AzureBlobReaderWriter().generate_presigned_url(
            "file.txt", datetime.timedelta(hours=1)
        )

        signing_kwargs = mock_generate_blob_sas.call_args.kwargs
        self.assertEqual(_TEST_ACCOUNT_KEY, signing_kwargs["account_key"])
        self.assertNotIn("user_delegation_key", signing_kwargs)
        # A user delegation key needs an Entra token, which is exactly what this mode lacks.
        service_client.get_user_delegation_key.assert_not_called()
        self.assertTrue(url.endswith("?sig=abc"))

    @patch.dict(os.environ, _TEST_ENVIRON_CONNECTION_STRING, clear=True)
    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    def test_presigned_url_without_an_account_key_is_rejected(self, mock_client_type):
        # A connection string can carry a SAS token instead of a shared key, leaving nothing to
        # sign with. The base class guards its own signing precondition.
        service_client = mock_client_type.from_connection_string.return_value
        service_client.get_blob_client.return_value.credential.account_key = None

        with self.assertRaises(ValueError) as context:
            AzureBlobReaderWriter().generate_presigned_url(
                "file.txt", datetime.timedelta(hours=1)
            )
        self.assertIn("account key", str(context.exception))

    @patch.dict(os.environ, _TEST_ENVIRON_CONNECTION_STRING, clear=True)
    @patch(
        "apollo.integrations.azure_blob.azure_blob_reader_writer.StorageManagementClient"
    )
    @patch(
        "apollo.integrations.azure_blob.azure_blob_base_reader_writer.BlobServiceClient"
    )
    def test_is_bucket_private_uses_the_shared_key_client(
        self, mock_client_type, mock_management_client_type
    ):
        service_client = mock_client_type.from_connection_string.return_value
        container_client = service_client.get_container_client.return_value
        container_client.get_container_access_policy.return_value = {
            "public_access": None
        }

        self.assertTrue(AzureBlobReaderWriter().is_bucket_private())

        service_client.get_container_client.assert_called_once_with(_TEST_BUCKET_NAME)
        # The management API needs both an Entra token and the WEBSITE_* vars that only exist on
        # App Service, so it must not be reached here.
        mock_management_client_type.assert_not_called()
