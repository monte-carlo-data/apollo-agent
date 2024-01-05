from unittest import TestCase
from unittest.mock import (
    Mock,
    patch,
)

from apollo.agent.agent import Agent
from apollo.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.aws.base_aws_proxy_client import BaseAwsProxyClient

_ATHENA_CREDENTIALS = {
    "assumable_role": "arn:aws:iam::foo:role/bar",
    "aws_region": "us-east-1",
    "external_id": "fizzbuzz",
}


class AthenaTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_client = Mock()

    @patch.object(BaseAwsProxyClient, "create_boto_client")
    def test_list_databases(self, mock_boto_client):
        mock_boto_client.return_value = self._mock_client
        databases = {"DatabaseList": [{"Name": "db-name"}]}
        self._mock_client.list_databases.return_value = databases
        result = self._agent.execute_operation(
            "athena",
            "list_databases",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [
                    {
                        "method": "list_databases",
                        "kwargs": {"CatalogName": "catalog-name"},
                    }
                ],
            },
            credentials=_ATHENA_CREDENTIALS,
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        response = result.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual(databases, response)
