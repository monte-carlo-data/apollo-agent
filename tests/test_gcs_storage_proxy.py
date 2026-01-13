import datetime
import os
from unittest import TestCase
from unittest.mock import patch, create_autospec, Mock, mock_open

from box import Box
from google.cloud.storage import Client, Bucket, Blob

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    PLATFORM_GCP,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR,
)
from apollo.common.agent.env_vars import (
    STORAGE_BUCKET_NAME_ENV_VAR,
    STORAGE_PREFIX_ENV_VAR,
    STORAGE_PREFIX_DEFAULT_VALUE,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.utils import AgentUtils
from apollo.interfaces.cloudrun.metadata_service import (
    GCP_PLATFORM_INFO_KEY_SERVICE_NAME,
    GCP_PLATFORM_INFO_KEY_PROJECT_ID,
    GCP_PLATFORM_INFO_KEY_REGION,
)
from apollo.interfaces.cloudrun.platform import CloudRunPlatformProvider

_TEST_BUCKET_NAME = "test_bucket"


class StorageGcsTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._agent.platform_provider = CloudRunPlatformProvider(
            platform_info={
                GCP_PLATFORM_INFO_KEY_SERVICE_NAME: "test-service",
                GCP_PLATFORM_INFO_KEY_PROJECT_ID: "test-project",
                GCP_PLATFORM_INFO_KEY_REGION: "test-region",
            }
        )

        self._mock_client = create_autospec(Client)
        self._mock_bucket = create_autospec(Bucket)
        self._mock_blob = create_autospec(Blob)

        self._mock_client.get_bucket.return_value = self._mock_bucket
        self._mock_bucket.blob.return_value = self._mock_blob

    @patch.dict(
        os.environ,
        {
            STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
            STORAGE_PREFIX_ENV_VAR: "",
        },
    )
    @patch("apollo.integrations.gcs.gcs_base_reader_writer.Client")
    def test_list_objects(self, mock_client_type):
        mock_client_type.return_value = self._mock_client
        pages = [
            [
                Box(
                    etag="123",
                    name="file_1.txt",
                    size=23,
                    updated=datetime.datetime.utcnow(),
                    storage_class="STANDARD",
                )
            ]
        ]
        list_blobs_result = Mock()
        list_blobs_result.pages = (p for p in pages)
        list_blobs_result.next_page_token = "12345"
        self._mock_client.list_blobs.return_value = list_blobs_result

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

        self._mock_client.list_blobs.assert_called_with(
            bucket_or_name=_TEST_BUCKET_NAME
        )

        response = result.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual("12345", response["page_token"])
        self.assertEqual(pages[0][0].name, response["list"][0]["Key"])
        self.assertEqual(pages[0][0].size, response["list"][0]["Size"])
        self.assertEqual(pages[0][0].etag, response["list"][0]["ETag"])
        self.assertEqual(pages[0][0].updated, response["list"][0]["LastModified"])
        self.assertEqual(pages[0][0].storage_class, response["list"][0]["StorageClass"])

    @patch.dict(
        os.environ,
        {STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME},
    )
    @patch("apollo.integrations.gcs.gcs_base_reader_writer.Client")
    def test_list_objects_default_prefix(self, mock_client_type):
        mock_client_type.return_value = self._mock_client
        expected_prefix = f"{STORAGE_PREFIX_DEFAULT_VALUE}/"
        file_name = "file_1.txt"
        pages = [
            [
                Box(
                    etag="123",
                    name=f"{expected_prefix}{file_name}",
                    size=23,
                    updated=datetime.datetime.utcnow(),
                    storage_class="STANDARD",
                )
            ]
        ]
        list_blobs_result = Mock()
        list_blobs_result.pages = (p for p in pages)
        list_blobs_result.next_page_token = "12345"
        self._mock_client.list_blobs.return_value = list_blobs_result

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

        self._mock_client.list_blobs.assert_called_with(
            bucket_or_name=_TEST_BUCKET_NAME,
            prefix=expected_prefix,
        )

        response = result.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual("12345", response["page_token"])
        self.assertEqual(file_name, response["list"][0]["Key"])
        self.assertEqual(pages[0][0].size, response["list"][0]["Size"])
        self.assertEqual(pages[0][0].etag, response["list"][0]["ETag"])
        self.assertEqual(pages[0][0].updated, response["list"][0]["LastModified"])
        self.assertEqual(pages[0][0].storage_class, response["list"][0]["StorageClass"])

    @patch.dict(
        os.environ,
        {
            STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
            STORAGE_PREFIX_ENV_VAR: "",
        },
    )
    @patch("apollo.integrations.gcs.gcs_base_reader_writer.Client")
    def test_delete(self, mock_client_type):
        mock_client_type.return_value = self._mock_client

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

        self._mock_bucket.blob.assert_called_with(file_key)
        self._mock_blob.delete.assert_called()

    @patch.dict(
        os.environ,
        {STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME},
    )
    @patch("apollo.integrations.gcs.gcs_base_reader_writer.Client")
    def test_delete_default_prefix(self, mock_client_type):
        mock_client_type.return_value = self._mock_client
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

        self._mock_bucket.blob.assert_called_with(f"{expected_prefix}{file_key}")
        self._mock_blob.delete.assert_called()

    @patch.dict(
        os.environ,
        {
            STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
            STORAGE_PREFIX_ENV_VAR: "",
        },
    )
    @patch("apollo.integrations.gcs.gcs_base_reader_writer.Client")
    def test_read(self, mock_client_type):
        mock_client_type.return_value = self._mock_client

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

        self._mock_bucket.blob.assert_called_with(file_key)
        self._mock_blob.download_as_bytes.assert_called()

    @patch.dict(
        os.environ,
        {STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME},
    )
    @patch("apollo.integrations.gcs.gcs_base_reader_writer.Client")
    def test_read_default_prefix(self, mock_client_type):
        mock_client_type.return_value = self._mock_client
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

        self._mock_bucket.blob.assert_called_with(f"{expected_prefix}{file_key}")
        self._mock_blob.download_as_bytes.assert_called()

    @patch.dict(
        os.environ,
        {
            STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
            STORAGE_PREFIX_ENV_VAR: "",
        },
    )
    @patch("apollo.integrations.gcs.gcs_base_reader_writer.Client")
    @patch.object(AgentUtils, "temp_file_path")
    def test_download(self, mock_temp_file_path, mock_client_type):
        tmp_path = "/tmp/temp.data"
        mock_temp_file_path.return_value = tmp_path
        mock_client_type.return_value = self._mock_client

        file_key = "file.txt"
        with patch("builtins.open", mock_open()) as open_file_mock:
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
        open_file_mock.assert_called_once_with(tmp_path, "rb")

        self._mock_bucket.blob.assert_called_with(file_key)
        self._mock_blob.download_to_filename.assert_called_with(tmp_path)
        mock_temp_file_path.assert_called_once()

    @patch.dict(
        os.environ,
        {STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME},
    )
    @patch("apollo.integrations.gcs.gcs_base_reader_writer.Client")
    @patch.object(AgentUtils, "temp_file_path")
    def test_download_default_prefix(self, mock_temp_file_path, mock_client_type):
        tmp_path = "/tmp/temp.data"
        mock_temp_file_path.return_value = tmp_path
        mock_client_type.return_value = self._mock_client
        expected_prefix = f"{STORAGE_PREFIX_DEFAULT_VALUE}/"

        file_key = "file.txt"
        with patch("builtins.open", mock_open()) as open_file_mock:
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
        open_file_mock.assert_called_once_with(tmp_path, "rb")

        self._mock_bucket.blob.assert_called_with(f"{expected_prefix}{file_key}")
        self._mock_blob.download_to_filename.assert_called_with(tmp_path)
        mock_temp_file_path.assert_called_once()
