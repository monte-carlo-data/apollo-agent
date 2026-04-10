from unittest import TestCase
from unittest.mock import Mock, patch

from apollo.integrations.aws.base_aws_proxy_client import AwsSession, BaseAwsProxyClient
from apollo.integrations.aws.asm_proxy_client import SecretsManagerProxyClient
from apollo.integrations.aws.glue_proxy_client import GlueProxyClient

_CREDS = {
    "assumable_role": "arn:aws:iam::123:role/test-role",
    "aws_region": "us-east-1",
    "external_id": "ext-id",
}


class TestBaseAwsProxyClientCredentialRouting(TestCase):
    """BaseAwsProxyClient (CCP-enabled integrations) reads AWS session params from connect_args."""

    @patch("apollo.integrations.aws.base_aws_proxy_client.boto3.Session")
    @patch.object(BaseAwsProxyClient, "_assume_role")
    def test_reads_from_connect_args(self, mock_assume_role, mock_session):
        mock_assume_role.return_value = AwsSession("KEY", "SECRET", "TOKEN")
        mock_session.return_value = Mock()

        GlueProxyClient({"connect_args": _CREDS})

        mock_assume_role.assert_called_once_with(
            assumable_role=_CREDS["assumable_role"],
            external_id=_CREDS["external_id"],
        )

    @patch("apollo.integrations.aws.base_aws_proxy_client.boto3.Session")
    def test_none_credentials(self, mock_session):
        mock_session.return_value = Mock()

        GlueProxyClient(None)

        mock_session.assert_called_once_with(region_name=None)


class TestSecretsManagerProxyClientCredentialRouting(TestCase):
    """
    SecretsManagerProxyClient is a credentials provider — its AWS session params are always
    at the top level of the credentials dict. connect_args, if present, holds downstream
    integration credentials and must not be used for the ASM session.
    """

    @patch("apollo.integrations.aws.base_aws_proxy_client.boto3.Session")
    @patch.object(BaseAwsProxyClient, "_assume_role")
    def test_reads_from_top_level(self, mock_assume_role, mock_session):
        mock_assume_role.return_value = AwsSession("KEY", "SECRET", "TOKEN")
        mock_session.return_value = Mock()

        SecretsManagerProxyClient(_CREDS)

        mock_assume_role.assert_called_once_with(
            assumable_role=_CREDS["assumable_role"],
            external_id=_CREDS["external_id"],
        )

    @patch("apollo.integrations.aws.base_aws_proxy_client.boto3.Session")
    @patch.object(BaseAwsProxyClient, "_assume_role")
    def test_ignores_connect_args_for_session(self, mock_assume_role, mock_session):
        """connect_args holds downstream creds — ASM session params must still come from top level."""
        mock_assume_role.return_value = AwsSession("KEY", "SECRET", "TOKEN")
        mock_session.return_value = Mock()

        credentials = {
            **_CREDS,
            "aws_secret": "my-secret",
            "connect_args": {"username": "user", "password": "pass"},
        }
        SecretsManagerProxyClient(credentials)

        mock_assume_role.assert_called_once_with(
            assumable_role=_CREDS["assumable_role"],
            external_id=_CREDS["external_id"],
        )
