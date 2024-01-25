from unittest import TestCase
from unittest.mock import (
    Mock,
    patch,
    create_autospec,
)

import boto3

from apollo.agent.agent import Agent
from apollo.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.aws.base_aws_proxy_client import BaseAwsProxyClient

_GLUE_CREDENTIALS = {
    "assumable_role": "arn:aws:iam::foo:role/bar",
    "aws_region": "us-east-1",
    "external_id": "fizzbuzz",
}


class GlueTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_client = Mock()

    @patch.object(BaseAwsProxyClient, "create_boto_client")
    def test_get_tables(self, mock_boto_client):
        mock_boto_client.return_value = self._mock_client
        tables = {"TableList": [{"CatalogId": "catalog-id", "DatabaseName": "db-name"}]}
        self._mock_client.get_tables.return_value = tables
        result = self._agent.execute_operation(
            "glue",
            "get_tables",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [
                    {"method": "get_tables", "kwargs": {"DatabaseName": "db-name"}}
                ],
            },
            credentials=_GLUE_CREDENTIALS,
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        response = result.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual(tables, response)
