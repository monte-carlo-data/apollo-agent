import os
from unittest import TestCase
from unittest.mock import patch, ANY

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR,
)
from apollo.common.agent.env_vars import (
    STORAGE_BUCKET_NAME_ENV_VAR,
    STORAGE_PREFIX_ENV_VAR,
    STORAGE_TYPE_ENV_VAR,
    STORAGE_ENDPOINT_URL_ENV_VAR,
    STORAGE_ACCESS_KEY_ENV_VAR,
    STORAGE_SECRET_KEY_ENV_VAR,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.common.agent.models import AgentConfigurationError
from apollo.integrations.s3_compatible.s3_compatible_reader_writer import (
    S3CompatibleReaderWriter,
)

_TEST_BUCKET_NAME = "test_bucket"
_TEST_ENDPOINT_URL = "http://localhost:9000"
_TEST_ACCESS_KEY = "minioadmin"
_TEST_SECRET_KEY = "minioadmin"


class StorageS3CompatibleTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())

    @patch.dict(
        os.environ,
        {
            STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
            STORAGE_PREFIX_ENV_VAR: "",
            STORAGE_TYPE_ENV_VAR: "S3_COMPATIBLE",
            STORAGE_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
            STORAGE_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
            STORAGE_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
        },
    )
    @patch.object(S3CompatibleReaderWriter, "s3_client")
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
            STORAGE_TYPE_ENV_VAR: "S3_COMPATIBLE",
            STORAGE_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
            STORAGE_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
            STORAGE_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
        },
    )
    @patch.object(S3CompatibleReaderWriter, "s3_client")
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
            STORAGE_TYPE_ENV_VAR: "S3_COMPATIBLE",
            STORAGE_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
            STORAGE_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
            STORAGE_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
        },
    )
    @patch("boto3.client")
    def test_client_configured_with_endpoint_and_credentials(self, mock_boto_client):
        rw = S3CompatibleReaderWriter()
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
            STORAGE_TYPE_ENV_VAR: "S3_COMPATIBLE",
            STORAGE_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
            STORAGE_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
            STORAGE_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
        },
    )
    @patch("boto3.client")
    def test_regional_client(self, mock_boto_client):
        rw = S3CompatibleReaderWriter()
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
            STORAGE_TYPE_ENV_VAR: "S3_COMPATIBLE",
            STORAGE_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
            STORAGE_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
            STORAGE_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
        },
    )
    @patch("boto3.resource")
    def test_resource_configured_with_endpoint_and_credentials(
        self, mock_boto_resource
    ):
        rw = S3CompatibleReaderWriter()
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
                STORAGE_TYPE_ENV_VAR: "S3_COMPATIBLE",
                STORAGE_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
                STORAGE_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
                STORAGE_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
            },
            clear=False,
        ):
            with self.assertRaises(AgentConfigurationError) as context:
                S3CompatibleReaderWriter()
            self.assertIn(STORAGE_BUCKET_NAME_ENV_VAR, str(context.exception))

    def test_missing_endpoint_url(self):
        with patch.dict(
            os.environ,
            {
                STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
                STORAGE_PREFIX_ENV_VAR: "",
                STORAGE_TYPE_ENV_VAR: "S3_COMPATIBLE",
                STORAGE_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
                STORAGE_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
            },
            clear=False,
        ):
            with self.assertRaises(AgentConfigurationError) as context:
                S3CompatibleReaderWriter()
            self.assertIn(STORAGE_ENDPOINT_URL_ENV_VAR, str(context.exception))

    def test_missing_access_key(self):
        with patch.dict(
            os.environ,
            {
                STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
                STORAGE_PREFIX_ENV_VAR: "",
                STORAGE_TYPE_ENV_VAR: "S3_COMPATIBLE",
                STORAGE_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
                STORAGE_SECRET_KEY_ENV_VAR: _TEST_SECRET_KEY,
            },
            clear=False,
        ):
            with self.assertRaises(AgentConfigurationError) as context:
                S3CompatibleReaderWriter()
            self.assertIn(STORAGE_ACCESS_KEY_ENV_VAR, str(context.exception))

    def test_missing_secret_key(self):
        with patch.dict(
            os.environ,
            {
                STORAGE_BUCKET_NAME_ENV_VAR: _TEST_BUCKET_NAME,
                STORAGE_PREFIX_ENV_VAR: "",
                STORAGE_TYPE_ENV_VAR: "S3_COMPATIBLE",
                STORAGE_ENDPOINT_URL_ENV_VAR: _TEST_ENDPOINT_URL,
                STORAGE_ACCESS_KEY_ENV_VAR: _TEST_ACCESS_KEY,
            },
            clear=False,
        ):
            with self.assertRaises(AgentConfigurationError) as context:
                S3CompatibleReaderWriter()
            self.assertIn(STORAGE_SECRET_KEY_ENV_VAR, str(context.exception))
