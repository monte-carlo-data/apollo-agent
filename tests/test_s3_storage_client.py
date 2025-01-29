import datetime
import os
from unittest import TestCase
from unittest.mock import patch, create_autospec, Mock, mock_open, ANY

from box import Box
from google.cloud.storage import Client, Bucket, Blob

from apollo.agent.agent import Agent
from apollo.agent.constants import (
    PLATFORM_GCP,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR,
)
from apollo.agent.env_vars import (
    STORAGE_BUCKET_NAME_ENV_VAR,
    STORAGE_PREFIX_ENV_VAR,
    STORAGE_PREFIX_DEFAULT_VALUE,
    STORAGE_TYPE_ENV_VAR,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.utils import AgentUtils
from apollo.integrations.s3.s3_reader_writer import S3ReaderWriter
from apollo.interfaces.cloudrun.metadata_service import (
    GCP_PLATFORM_INFO_KEY_SERVICE_NAME,
    GCP_PLATFORM_INFO_KEY_PROJECT_ID,
    GCP_PLATFORM_INFO_KEY_REGION,
)
from apollo.interfaces.cloudrun.platform import CloudRunPlatformProvider

_TEST_BUCKET_NAME = "test_bucket"


class StorageS3Tests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())

    @patch.dict(
        os.environ,
        {
            STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
            STORAGE_PREFIX_ENV_VAR: "",
            STORAGE_TYPE_ENV_VAR: "S3",
        },
    )
    @patch.object(S3ReaderWriter, "s3_client")
    def test_list_objects(self, mock_s3_client):
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

        mock_s3_client.list_objects_v2.assert_called_with(Bucket=_TEST_BUCKET_NAME)

    @patch.dict(
        os.environ,
        {
            STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
            STORAGE_PREFIX_ENV_VAR: "",
            STORAGE_TYPE_ENV_VAR: "S3",
        },
    )
    @patch.object(S3ReaderWriter, "s3_client")
    def test_write(self, mock_s3_client):
        file_key = "test_file"
        result = self._agent.execute_operation(
            "storage",
            "write",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [
                    {
                        "method": "write",
                        "kwargs": {"key": file_key, "obj_to_write": "test"},
                    }
                ],
            },
            credentials={},
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        mock_s3_client.put_object.assert_called_with(
            Bucket=_TEST_BUCKET_NAME,
            Key=file_key,
            Body="test",
        )

    @patch.dict(
        os.environ,
        {
            STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
            STORAGE_PREFIX_ENV_VAR: "",
            STORAGE_TYPE_ENV_VAR: "S3",
        },
    )
    @patch("boto3.client")
    def test_regional_client(self, mock_boto_client):
        rw = S3ReaderWriter()
        _ = rw.s3_regional_client
        mock_boto_client.assert_called_with(
            "s3",
            endpoint_url=ANY,
            config=ANY,
        )
        self.assertEqual(
            "s3v4", mock_boto_client.call_args[1]["config"].signature_version
        )
