from unittest import TestCase
from unittest.mock import (
    MagicMock,
    create_autospec,
    patch,
)

from requests import Response
from tableauserverclient.server.endpoint.auth_endpoint import Auth
from tableauserverclient.server.endpoint.metadata_endpoint import Metadata
from tableauserverclient.server.server import Server

from apollo.agent.agent import Agent
from apollo.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.tableau.tableau_proxy_client import JwtAuth

_TABLEAU_CREDENTIALS = {
    "server_name": "https://prod-useast-a.online.tableau.com",
    "site_name": "mc_dev",
    "username": "test@example.com",
    "client_id": "client_id",
    "secret_id": "secret_id",
    "secret_value": "secret_value",
}


class TableauTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_creds = create_autospec(JwtAuth)
        self._mock_auth = create_autospec(Auth)
        self._mock_metadata = create_autospec(Metadata)
        self._mock_client = create_autospec(Server)
        self._mock_client.auth = self._mock_auth
        self._mock_client.metadata = self._mock_metadata

    @patch("apollo.integrations.tableau.tableau_proxy_client.JwtAuth")
    @patch("apollo.integrations.tableau.tableau_proxy_client.Server")
    @patch("apollo.integrations.tableau.tableau_proxy_client.generate_jwt")
    def test_metadata_query(
        self,
        mock_jwt_gen: MagicMock,
        mock_server_init: MagicMock,
        mock_creds_init: MagicMock,
    ):
        mock_jwt_gen.return_value = "fake_jwt"
        mock_server_init.return_value = self._mock_client
        mock_creds_init.return_value = self._mock_creds
        self._mock_metadata.query.return_value = {"foo": ["bar", "baz"]}

        result = self._agent.execute_operation(
            "tableau",
            "_query",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [
                    {
                        "method": "metadata_query",
                        "kwargs": {
                            "query": "graphql_query",
                            "variables": {"first": 1},
                            "abort_on_error": True,
                        },
                    }
                ],
            },
            credentials=_TABLEAU_CREDENTIALS,
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        response = result.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual({"foo": ["bar", "baz"]}, response)
        self._mock_client.auth.sign_in.assert_called_once_with(self._mock_creds)
        self._mock_metadata.query.assert_called_once_with(
            query="graphql_query", variables={"first": 1}, abort_on_error=True
        )

    @patch("apollo.integrations.tableau.tableau_proxy_client.JwtAuth")
    @patch("apollo.integrations.tableau.tableau_proxy_client.Server")
    @patch("apollo.integrations.tableau.tableau_proxy_client.generate_jwt")
    @patch("requests.request")
    def test_api_request(
        self, mock_request, mock_jwt_gen, mock_server_init, mock_creds_init
    ):
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = (
            "<tsResponse>\n\t<views>\n"
            '\t\t<view id="view-id" name="view-name" contentUrl="content-url">\n'
            '\t\t\t<workbook id="workbook-id" />\n\t\t\t<owner id="owner-id" />\n'
            '\t\t\t<usage totalViewCount="total-count" />\n\t\t</view>\n\t</views>\n</tsResponse>'
        )
        mock_response.status_code = 200
        mock_response.text = expected_result

        mock_jwt_gen.return_value = "fake_jwt"
        mock_server_init.return_value = self._mock_client
        mock_creds_init.return_value = self._mock_creds
        self._mock_client.baseurl = "https://example.com"
        self._mock_client.site_id = "sample_site_id"
        self._mock_client.auth_token = "fizz|buzz|sample_site_id"

        result = self._agent.execute_operation(
            "tableau",
            "_query",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [
                    {
                        "method": "api_request",
                        "kwargs": {
                            "path": "views?includeUsageStatistics=true",
                            "request_method": "GET",
                            "content_type": "application/xml",
                            "params": {"pageNumber": 1, "pageSize": 10},
                        },
                    }
                ],
            },
            credentials=_TABLEAU_CREDENTIALS,
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        response = result.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual((expected_result, 200), response)
        self._mock_client.auth.sign_in.assert_called_once_with(self._mock_creds)
        mock_request.assert_called_once_with(
            method="GET",
            url="https://example.com/sites/sample_site_id/views?includeUsageStatistics=true",
            data=None,
            headers={
                "X-Tableau-Auth": "fizz|buzz|sample_site_id",
                "Content-Type": "application/xml",
            },
            params={"pageNumber": 1, "pageSize": 10},
        )

    @patch("apollo.integrations.tableau.tableau_proxy_client.JwtAuth")
    @patch("apollo.integrations.tableau.tableau_proxy_client.Server")
    @patch("apollo.integrations.tableau.tableau_proxy_client.generate_jwt")
    @patch("requests.request")
    def test_api_request_with_url(
        self, mock_request, mock_jwt_gen, mock_server_init, mock_creds_init
    ):
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = (
            "<tsResponse>\n\t<views>\n"
            '\t\t<view id="view-id" name="view-name" contentUrl="content-url">\n'
            '\t\t\t<workbook id="workbook-id" />\n\t\t\t<owner id="owner-id" />\n'
            '\t\t\t<usage totalViewCount="total-count" />\n\t\t</view>\n\t</views>\n</tsResponse>'
        )
        mock_response.status_code = 200
        mock_response.text = expected_result

        mock_jwt_gen.return_value = "fake_jwt"
        mock_server_init.return_value = self._mock_client
        mock_creds_init.return_value = self._mock_creds
        self._mock_client.baseurl = "https://example.com"
        self._mock_client.site_id = "sample_site_id"
        self._mock_client.auth_token = "fizz|buzz|sample_site_id"

        result = self._agent.execute_operation(
            "tableau",
            "_query",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [
                    {
                        "method": "api_request",
                        "kwargs": {
                            "path": "https://example.com/sites/sample_site_id/views?includeUsageStatistics=true",
                            "request_method": "GET",
                            "content_type": "application/xml",
                            "params": {"pageNumber": 1, "pageSize": 10},
                        },
                    }
                ],
            },
            credentials=_TABLEAU_CREDENTIALS,
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        response = result.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual((expected_result, 200), response)
        self._mock_client.auth.sign_in.assert_called_once_with(self._mock_creds)
        mock_request.assert_called_once_with(
            method="GET",
            url="https://example.com/sites/sample_site_id/views?includeUsageStatistics=true",
            data=None,
            headers={
                "X-Tableau-Auth": "fizz|buzz|sample_site_id",
                "Content-Type": "application/xml",
            },
            params={"pageNumber": 1, "pageSize": 10},
        )
