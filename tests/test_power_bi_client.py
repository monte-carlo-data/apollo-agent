from unittest import TestCase
from unittest.mock import create_autospec, call

from requests import Response

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_RESULT,
)
from apollo.agent.logging_utils import LoggingUtils

# CTP path: MSAL token is pre-resolved by the pipeline; proxy client receives connect_args.
_POWER_BI_CREDENTIALS = {
    "connect_args": {
        "token": "test-bearer-token",
        "auth_type": "Bearer",
    }
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

    def test_http_request(self):
        import unittest.mock as mock

        mock_response = create_autospec(Response)
        mock_response.json.return_value = {"ok": True}

        with mock.patch("requests.request", return_value=mock_response) as mock_request:
            response = self._agent.execute_operation(
                connection_type="power-bi",
                operation_name="do_request",
                operation_dict=_HTTP_OPERATION,
                credentials=_POWER_BI_CREDENTIALS,
            )

        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": "Bearer test-bearer-token",
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
        self.assertEqual({"ok": True}, response.result.get(ATTRIBUTE_NAME_RESULT))
