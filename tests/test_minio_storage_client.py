import os
from unittest import TestCase
from unittest.mock import patch, ANY

from apollo.agent.agent import Agent
from apollo.agent.constants import (
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR,
)
from apollo.agent.env_vars import (
    STORAGE_BUCKET_NAME_ENV_VAR,
    STORAGE_PREFIX_ENV_VAR,
    STORAGE_TYPE_ENV_VAR,
    MINIO_ENDPOINT_URL_ENV_VAR,
    MINIO_ACCESS_KEY_ENV_VAR,
    MINIO_SECRET_KEY_ENV_VAR,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.models import AgentConfigurationError
from apollo.integrations.minio.minio_reader_writer import MinIOReaderWriter

_TEST_BUCKET_NAME = "test_bucket"
_TEST_ENDPOINT_URL = "http://localhost:9000"
_TEST_ACCESS_KEY = "minioadmin"
_TEST_SECRET_KEY = "minioadmin"


class StorageMinIOTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())

    @patch.dict(
        os.environ,
        {
            STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
            STORAGE_PREFIX_ENV_VAR: "",
            STORAGE_TYPE_ENV_VAR: "MINIO",
            MINIO_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
            MINIO_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
            MINIO_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
        },
    )
    @patch.object(MinIOReaderWriter, "s3_client")
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
            STORAGE_TYPE_ENV_VAR: "MINIO",
            MINIO_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
            MINIO_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
            MINIO_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
        },
    )
    @patch.object(MinIOReaderWriter, "s3_client")
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
            STORAGE_TYPE_ENV_VAR: "MINIO",
            MINIO_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
            MINIO_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
            MINIO_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
        },
    )
    @patch("boto3.client")
    def test_client_configured_with_endpoint_and_credentials(self, mock_boto_client):
        rw = MinIOReaderWriter()
        _ = rw.s3_client
        mock_boto_client.assert_called_with(
            "s3",
            endpoint_url=_TEST_ENDPOINT_URL,
            aws_access_key_id=_TEST_ACCESS_KEY,
            aws_secret_access_key=_TEST_SECRET_KEY,
        )

    @patch.dict(
        os.environ,
        {
            STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
            STORAGE_PREFIX_ENV_VAR: "",
            STORAGE_TYPE_ENV_VAR: "MINIO",
            MINIO_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
            MINIO_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
            MINIO_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
        },
    )
    @patch("boto3.client")
    def test_regional_client(self, mock_boto_client):
        rw = MinIOReaderWriter()
        _ = rw.s3_regional_client
        mock_boto_client.assert_called_with(
            "s3",
            endpoint_url=_TEST_ENDPOINT_URL,
            aws_access_key_id=_TEST_ACCESS_KEY,
            aws_secret_access_key=_TEST_SECRET_KEY,
            config=ANY,
        )
        self.assertEqual(
            "s3v4", mock_boto_client.call_args[1]["config"].signature_version
        )

    @patch.dict(
        os.environ,
        {
            STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
            STORAGE_PREFIX_ENV_VAR: "",
            STORAGE_TYPE_ENV_VAR: "MINIO",
            MINIO_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
            MINIO_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
            MINIO_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
        },
    )
    @patch("boto3.resource")
    def test_resource_configured_with_endpoint_and_credentials(
        self, mock_boto_resource
    ):
        rw = MinIOReaderWriter()
        _ = rw.s3_resource
        mock_boto_resource.assert_called_with(
            "s3",
            endpoint_url=_TEST_ENDPOINT_URL,
            aws_access_key_id=_TEST_ACCESS_KEY,
            aws_secret_access_key=_TEST_SECRET_KEY,
        )

    def test_missing_bucket_name(self):
        with patch.dict(
            os.environ,
            {
                STORAGE_PREFIX_ENV_VAR: "",
                STORAGE_TYPE_ENV_VAR: "MINIO",
                MINIO_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
                MINIO_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
                MINIO_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
            },
            clear=False,
        ):
            with self.assertRaises(AgentConfigurationError) as context:
                MinIOReaderWriter()
            self.assertIn(STORAGE_BUCKET_NAME_ENV_VAR, str(context.exception))

    def test_missing_endpoint_url(self):
        with patch.dict(
            os.environ,
            {
                STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
                STORAGE_PREFIX_ENV_VAR: "",
                STORAGE_TYPE_ENV_VAR: "MINIO",
                MINIO_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
                MINIO_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
            },
            clear=False,
        ):
            with self.assertRaises(AgentConfigurationError) as context:
                MinIOReaderWriter()
            self.assertIn(MINIO_ENDPOINT_URL_ENV_VAR, str(context.exception))

    def test_missing_access_key(self):
        with patch.dict(
            os.environ,
            {
                STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
                STORAGE_PREFIX_ENV_VAR: "",
                STORAGE_TYPE_ENV_VAR: "MINIO",
                MINIO_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
                MINIO_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
            },
            clear=False,
        ):
            with self.assertRaises(AgentConfigurationError) as context:
                MinIOReaderWriter()
            self.assertIn(MINIO_ACCESS_KEY_ENV_VAR, str(context.exception))

    def test_missing_secret_key(self):
        with patch.dict(
            os.environ,
            {
                STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
                STORAGE_PREFIX_ENV_VAR: "",
                STORAGE_TYPE_ENV_VAR: "MINIO",
                MINIO_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
                MINIO_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
            },
            clear=False,
        ):
            with self.assertRaises(AgentConfigurationError) as context:
                MinIOReaderWriter()
            self.assertIn(MINIO_SECRET_KEY_ENV_VAR, str(context.exception))
