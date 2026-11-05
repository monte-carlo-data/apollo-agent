import os
from typing import Dict, List, Optional
from unittest import TestCase
from unittest.mock import Mock, patch

from botocore.client import BaseClient
from botocore.config import Config

from apollo.integrations.aws.aws_utils import AwsSession, assume_role
from apollo.integrations.aws.base_aws_proxy_client import BaseAwsProxyClient

_ROLE = "arn:aws:iam::123456789012:role/narrow"
_STS_RESPONSE = {
    "Credentials": {
        "AccessKeyId": "AKIA_TEST",
        "SecretAccessKey": "secret",
        "SessionToken": "token",
    }
}


@patch("apollo.integrations.aws.aws_utils.create_sts_client")
class TestAssumeRole(TestCase):
    def _sts(self, mock_client: Mock) -> Mock:
        sts = Mock()
        sts.assume_role.return_value = _STS_RESPONSE
        mock_client.return_value = sts
        return sts

    def test_returns_session_credentials(self, mock_client: Mock):
        self._sts(mock_client)

        session = assume_role(_ROLE)

        self.assertEqual(AwsSession("AKIA_TEST", "secret", "token"), session)
        mock_client.assert_called_once_with(config=None)

    def test_config_is_passed_to_the_sts_client(self, mock_client: Mock):
        self._sts(mock_client)
        config = Config(connect_timeout=3, read_timeout=3)

        assume_role(_ROLE, config=config)

        mock_client.assert_called_once_with(config=config)

    def test_explicit_session_name_and_external_id(self, mock_client: Mock):
        sts = self._sts(mock_client)

        assume_role(_ROLE, external_id="ext", session_name="mcd_mcp_abc")

        sts.assume_role.assert_called_once_with(
            RoleArn=_ROLE, RoleSessionName="mcd_mcp_abc", ExternalId="ext"
        )

    def test_default_session_name_is_unique_and_external_id_omitted(
        self, mock_client: Mock
    ):
        sts = self._sts(mock_client)

        assume_role(_ROLE)
        assume_role(_ROLE)

        first, second = [c.kwargs for c in sts.assume_role.call_args_list]
        self.assertNotIn("ExternalId", first)
        self.assertTrue(first["RoleSessionName"].startswith("mcd_"))
        self.assertNotEqual(first["RoleSessionName"], second["RoleSessionName"])

    def test_sts_errors_propagate(self, mock_client: Mock):
        sts = self._sts(mock_client)
        sts.assume_role.side_effect = RuntimeError("AccessDenied")

        with self.assertRaises(RuntimeError):
            assume_role(_ROLE)

    def test_base_aws_proxy_client_delegates(self, mock_client: Mock):
        sts = self._sts(mock_client)

        session = BaseAwsProxyClient._assume_role(
            assumable_role=_ROLE, external_id="ext"
        )

        self.assertEqual(AwsSession("AKIA_TEST", "secret", "token"), session)
        kwargs = sts.assume_role.call_args.kwargs
        self.assertEqual("ext", kwargs["ExternalId"])
        self.assertTrue(kwargs["RoleSessionName"].startswith("mcd_"))


def sts_endpoints_used(region: Optional[str], call) -> List[str]:
    """
    Runs `call` with real botocore clients but no network, and returns the endpoint
    URL of every STS request it made. Isolated from local AWS config and env.
    """
    env: Dict[str, str] = {
        "AWS_ACCESS_KEY_ID": "AKIA_TEST",
        "AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_CONFIG_FILE": os.devnull,
        "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
    }
    if region:
        env["AWS_REGION"] = region
        env["AWS_DEFAULT_REGION"] = region
    endpoints: List[str] = []

    def fake_api_call(client: BaseClient, operation_name: str, api_params: Dict):
        if client.meta.service_model.service_name == "sts":
            endpoints.append(client.meta.endpoint_url)
        return _STS_RESPONSE

    with patch.dict(os.environ, env, clear=True), patch.object(
        BaseClient, "_make_api_call", autospec=True, side_effect=fake_api_call
    ):
        call()
    return endpoints


class TestStsEndpoint(TestCase):
    """
    The agent must call the regional STS endpoint: the VPC interface endpoint for
    STS only serves the regional hostname, so the global sts.amazonaws.com fails
    when the agent's egress is limited to VPC endpoints.
    """

    def test_assume_role_uses_the_regional_endpoint(self):
        endpoints = sts_endpoints_used("us-west-2", lambda: assume_role(_ROLE))

        self.assertEqual(["https://sts.us-west-2.amazonaws.com"], endpoints)

    def test_assume_role_uses_the_partition_endpoint(self):
        endpoints = sts_endpoints_used("us-gov-west-1", lambda: assume_role(_ROLE))

        self.assertEqual(["https://sts.us-gov-west-1.amazonaws.com"], endpoints)

    def test_assume_role_without_a_region_uses_the_global_endpoint(self):
        endpoints = sts_endpoints_used(None, lambda: assume_role(_ROLE))

        self.assertEqual(["https://sts.amazonaws.com"], endpoints)
