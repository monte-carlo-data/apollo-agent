from unittest import TestCase
from unittest.mock import Mock, patch

from apollo.agent import constants
from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.aws.base_aws_proxy_client import BaseAwsProxyClient


class MskConnectClientTests(TestCase):
    _CREDENTIALS = {
        "assumable_role": "arn:aws:iam::account_id:role/test",
        "aws_region": "us-east-1",
        "external_id": "foo",
    }

    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_client = Mock()

    @patch.object(BaseAwsProxyClient, "create_boto_client")
    def test_operation(self, mock_boto_client: Mock):
        # given
        mock_boto_client.return_value = self._mock_client
        operation_result = {"foo": "bar"}
        self._mock_client.list_connectors.return_value = operation_result

        # when
        response = self._agent.execute_operation(
            "msk-connect",
            "list_connectors",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [
                    {"method": "list_connectors", "kwargs": {"maxResults": 10}}
                ],
            },
            credentials=self._CREDENTIALS,
        )

        # then
        self.assertIsNone(response.result.get(constants.ATTRIBUTE_NAME_ERROR))
        self.assertEqual(
            operation_result, response.result.get(constants.ATTRIBUTE_NAME_RESULT)
        )
        self._mock_client.list_connectors.assert_called_once_with(maxResults=10)
