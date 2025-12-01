from unittest import TestCase
from unittest.mock import (
    Mock,
    patch,
)

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.aws.base_aws_proxy_client import BaseAwsProxyClient, AwsSession
from apollo.integrations.aws.glue_proxy_client import GlueProxyClient

_GLUE_CREDENTIALS = {
    "assumable_role": "arn:aws:iam::foo:role/bar",
    "aws_region": "us-east-1",
    "external_id": "fizzbuzz",
}
_GLUE_CREDENTIALS_WITH_CERT = {**_GLUE_CREDENTIALS, "ssl_options": {"ca_data": "cert"}}


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

    @patch("apollo.integrations.aws.base_aws_proxy_client.boto3.Session")
    @patch.object(BaseAwsProxyClient, "_assume_role")
    def test_init_with_cert(self, mock_assume_role, mock_session):
        # mock assume_role
        mock_assume_role.return_value = AwsSession("AKIA...", "SECRET", "TOKEN")

        # mock boto3 session and client
        mock_boto_client = Mock()
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_boto_client
        mock_session.return_value = mock_session_instance

        GlueProxyClient(_GLUE_CREDENTIALS_WITH_CERT)

        mock_session_instance.client.assert_called_once_with(
            "glue", verify="/tmp/glue_ca_bundle.pem"
        )
