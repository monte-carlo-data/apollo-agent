from unittest import TestCase
from unittest.mock import create_autospec, patch, call

from msal import (
    ConfidentialClientApplication,
    PublicClientApplication,
)
from requests import Response

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_RESULT,
)
from apollo.agent.logging_utils import LoggingUtils

_POWER_BI_CREDENTIALS_SERVICE_PRINCIPAL = {
    "auth_mode": "service_principal",
    "client_id": "foo",
    "client_secret": "bar",
    "tenant_id": "baz",
}
_POWER_BI_CREDENTIALS_PRIMARY_USER = {
    "auth_mode": "primary_user",
    "client_id": "fizz",
    "tenant_id": "buzz",
    "username": "foo",
    "password": "bar",
}
_HTTP_OPERATION = {
    "trace_id": "1234",
    "commands": [
        {
            "method": "do_request",
            "kwargs": {
                "url": "https://test.com/path",
                "http_method": "GET",
                "payload": {},
                "additional_headers": {"Content-Type": "application/json"},
            },
        }
    ],
}


class TestPowerBiClient(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())

    @patch("requests.request")
    @patch(
        "apollo.integrations.powerbi.powerbi_proxy_client.msal.PublicClientApplication"
    )
    def test_http_request_with_primary_user(self, mock_client_app_init, mock_request):
        mock_client_app = create_autospec(PublicClientApplication)
        mock_client_app_init.return_value = mock_client_app
        mock_client_app.get_accounts.return_value = [{"username": "foo"}]
        mock_client_app.acquire_token_silent.return_value = None
        mock_client_app.acquire_token_by_username_password.return_value = {
            "access_token": "fizz"
        }
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        response = self._agent.execute_operation(
            connection_type="power-bi",
            operation_name="do_request",
            operation_dict=_HTTP_OPERATION,
            credentials=_POWER_BI_CREDENTIALS_PRIMARY_USER,
        )
        mock_client_app_init.assert_called_once_with(
            "fizz", authority="https://login.microsoftonline.com/buzz"
        )
        mock_client_app.get_accounts.assert_called_once_with(username="foo")
        mock_client_app.acquire_token_silent.assert_called_once_with(
            scopes=["https://analysis.windows.net/powerbi/api/.default"],
            account={"username": "foo"},
        )
        mock_client_app.acquire_token_by_username_password.assert_called_once_with(
            username="foo",
            password="bar",
            scopes=["https://analysis.windows.net/powerbi/api/.default"],
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": "Bearer fizz",
                "Content-Type": "application/json",
            },
        )
        mock_response.assert_has_calls(
            [
                call.raise_for_status(),
                call.json(),
            ]
        )
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        self.assertEqual(expected_result, response.result.get(ATTRIBUTE_NAME_RESULT))

    @patch("requests.request")
    @patch(
        "apollo.integrations.powerbi.powerbi_proxy_client.msal.ConfidentialClientApplication"
    )
    def test_http_request_with_service_principal(
        self, mock_client_app_init, mock_request
    ):
        mock_client_app = create_autospec(ConfidentialClientApplication)
        mock_client_app_init.return_value = mock_client_app
        mock_client_app.acquire_token_for_client.return_value = {
            "access_token": "foobar"
        }
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        response = self._agent.execute_operation(
            connection_type="power-bi",
            operation_name="do_request",
            operation_dict=_HTTP_OPERATION,
            credentials=_POWER_BI_CREDENTIALS_SERVICE_PRINCIPAL,
        )
        mock_client_app_init.assert_called_once_with(
            client_id="foo",
            client_credential="bar",
            authority="https://login.microsoftonline.com/baz",
        )
        mock_client_app.acquire_token_for_client.assert_called_once_with(
            scopes=["https://analysis.windows.net/powerbi/api/.default"]
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": "Bearer foobar",
                "Content-Type": "application/json",
            },
        )
        mock_response.assert_has_calls(
            [
                call.raise_for_status(),
                call.json(),
            ]
        )
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        self.assertEqual(expected_result, response.result.get(ATTRIBUTE_NAME_RESULT))
