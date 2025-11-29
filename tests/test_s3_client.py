import base64
import gzip
from io import BytesIO
from unittest import TestCase
from unittest.mock import patch, Mock

from botocore.response import StreamingBody

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_VALUE_TYPE_STREAMING_BODY,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.aws.base_aws_proxy_client import BaseAwsProxyClient

_S3_OPERATION = {
    "trace_id": "1234",
    "commands": [
        {
            "method": "get_object",
            "kwargs": {
                "path": "s3://bucket/path",
                "encoding": "utf-8",
            },
        }
    ],
}

_S3_CREDENTIALS = {
    "assumable_role": "arn:aws:iam::foo:role/bar",
    "aws_region": "us-east-1",
    "external_id": "fizzbuzz",
}


class TestS3Client(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_client = Mock()

    @patch.object(BaseAwsProxyClient, "create_boto_client")
    def test_get_object(self, mock_boto_client):
        mock_boto_client.return_value = self._mock_client

        # Create a real BytesIO object to wrap
        bytes_io_data = BytesIO("body".encode("utf8"))

        body = Mock(
            spec=StreamingBody, wraps=StreamingBody(bytes_io_data, len(b"body"))
        )
        body.read.side_effect = [b"bo", b"dy", None]

        object_result = {"Body": body}
        self._mock_client.get_object.return_value = object_result
        result = self._agent.execute_operation(
            "s3",
            "get_object",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [
                    {
                        "method": "get_object",
                        "kwargs": {
                            "Bucket": "s3://bucket/path",
                            "encoding": "utf-8",
                        },
                    }
                ],
            },
            credentials=_S3_CREDENTIALS,
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        response = result.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual(
            {
                "Body": {
                    "__data__": base64.b64encode(gzip.compress(b"body")).decode(
                        "ascii"
                    ),
                    # Expected base64 encoded gzip string
                    "__type__": ATTRIBUTE_VALUE_TYPE_STREAMING_BODY,
                }
            },
            response,
        )
