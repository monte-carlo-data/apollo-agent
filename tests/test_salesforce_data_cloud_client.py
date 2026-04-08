import json
import uuid
from unittest import TestCase
from unittest.mock import Mock, patch

import responses

from apollo.agent.agent import Agent
from apollo.common.agent.constants import ATTRIBUTE_NAME_RESULT
from apollo.agent.logging_utils import LoggingUtils


class SalesforceDataCloudProxyClientTests(TestCase):
    def setUp(self):
        self.agent = Agent(LoggingUtils())
        self.credentials = {
            "connect_args": {
                "domain": "test.salesforce.com",
                "client_id": "test_client_id",
                "client_secret": "test_client_secret",
                "core_token": "test_core_token",  # Default is client credentials which only has core_token
            }
        }

        self.mock_responses = responses.RequestsMock()
        self.mock_responses.start()

        self.addCleanup(self.mock_responses.stop)
        self.addCleanup(self.mock_responses.reset)

        self.setup_salesforce_data_cloud_api()

    def setup_salesforce_data_cloud_api(self):
        self.metadata_response = [
            {
                "name": "Account",
                "displayName": "Account",
                "fields": [
                    {"name": "Id", "displayName": "Id", "type": "STRING"},
                    {"name": "Name", "displayName": "Name", "type": "STRING"},
                    {
                        "name": "CreatedDate",
                        "displayName": "Created Date",
                        "type": "DATE_TIME",
                    },
                ],
            },
            {
                "name": "Contact",
                "displayName": "Contact",
                "fields": [
                    {"name": "Id", "displayName": "Id", "type": "STRING"},
                    {
                        "name": "FirstName",
                        "displayName": "First Name",
                        "type": "STRING",
                    },
                    {"name": "LastName", "displayName": "Last Name", "type": "STRING"},
                    {"name": "Email", "displayName": "Email", "type": "STRING"},
                ],
            },
            {
                "name": "Opportunity",
                "displayName": "Opportunity",
                "fields": [
                    {"name": "Id", "displayName": "Id", "type": "STRING"},
                    {"name": "Amount", "displayName": "Amount", "type": "DECIMAL"},
                    {
                        "name": "CloseDate",
                        "displayName": "Close Date",
                        "type": "DATE_TIME",
                    },
                ],
            },
        ]

        self.data_response = {
            "data": [
                ["Account1", "Active", "2021-09-16T16:26:36+00:00"],
                ["Account2", "Inactive", "2023-01-02T14:20:00+00:00"],
            ],
            "startTime": "2022-03-07T19:57:19.374525Z",
            "endTime": "2022-03-07T19:57:20.063372Z",
            "rowCount": 3,
            "queryId": "20220307_195719_00109_5frjj",
            "nextBatchId": "fa489494-ff42-45ce-afd6-b838854b5a99",
            "done": True,
            "metadata": {
                "Name": {
                    "type": "VARCHAR",
                    "placeInOrder": 0,
                },
                "Status": {
                    "type": "VARCHAR",
                    "placeInOrder": 1,
                },
                "CreatedDate": {
                    "type": "TIMESTAMP",
                    "placeInOrder": 2,
                },
            },
        }

        self.client_credentials_token = str(uuid.uuid4())
        self.api_token = str(uuid.uuid4())

        self.client_credentials_token_endpoint = Mock(
            return_value=(
                200,
                {},
                json.dumps(
                    {
                        "access_token": self.client_credentials_token,
                        "instance_url": "https://test.salesforce.com",
                    }
                ),
            )
        )
        self.mock_responses.add_callback(
            method=responses.POST,
            url="https://test.salesforce.com/services/oauth2/token",
            callback=self.client_credentials_token_endpoint,
        )

        # The library revokes the core token after exchange in the client credentials flow.
        self.mock_responses.add(
            method=responses.POST,
            url="https://test.salesforce.com/services/oauth2/revoke",
            status=200,
            body="",
        )

        self.api_token_endpoint = Mock(
            return_value=(
                200,
                {},
                json.dumps(
                    {
                        "access_token": self.api_token,
                        "expires_in": 3600,
                        "instance_url": "test.salesforce.com",
                    }
                ),
            )
        )
        self.mock_responses.add_callback(
            method=responses.POST,
            url="https://test.salesforce.com/services/a360/token",
            callback=self.api_token_endpoint,
        )

        self.metadata_endpoint = Mock(
            return_value=(200, {}, json.dumps({"metadata": self.metadata_response}))
        )
        self.mock_responses.add_callback(
            method=responses.GET,
            url="https://test.salesforce.com/api/v1/metadata",
            callback=self.metadata_endpoint,
        )

        self.query_endpoint = Mock(
            return_value=(200, {}, json.dumps(self.data_response))
        )
        self.mock_responses.add_callback(
            method=responses.POST,
            url="https://test.salesforce.com/api/v2/query",
            callback=self.query_endpoint,
        )

    def test_init(self):
        # Old DC path: core_token is provided by the data-collector.
        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,  # Force a new client to be created
            "commands": [{"method": "_connection_type"}],
        }

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_init",
            operation_dict=operation,
            credentials=self.credentials,
        )

        self.assertFalse(response.is_error)
        self.assertEqual(
            response.result[ATTRIBUTE_NAME_RESULT], "salesforce-data-cloud"
        )

    def test_init_with_client_credentials_flow(self):
        # New DC path: only client_id/client_secret, no core_token.
        # The library handles OAuth + exchange internally via _token_by_client_creds_flow.
        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,
            "commands": [{"method": "_connection_type"}],
        }

        del self.credentials["connect_args"]["core_token"]

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_init_clean",
            operation_dict=operation,
            credentials=self.credentials,
        )

        self.assertFalse(response.is_error)
        self.assertEqual(
            response.result[ATTRIBUTE_NAME_RESULT], "salesforce-data-cloud"
        )

    def test_init_with_refresh_token(self):
        # Backward compat: old DCs sent refresh_token="required_but_not_used".
        # This is normalized to None → same as new clean path.
        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,
            "commands": [{"method": "_connection_type"}],
        }

        del self.credentials["connect_args"]["core_token"]
        self.credentials["connect_args"]["refresh_token"] = "required_but_not_used"

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_init",
            operation_dict=operation,
            credentials=self.credentials,
        )

        self.assertFalse(response.is_error)
        self.assertEqual(
            response.result[ATTRIBUTE_NAME_RESULT], "salesforce-data-cloud"
        )

    def test_list_tables(self):
        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,  # Force a new client to be created
            "commands": [{"method": "list_tables"}],
        }

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_list_tables",
            operation_dict=operation,
            credentials=self.credentials,
        )

        tables = response.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual(len(tables), len(self.metadata_response))

        for mock_table in self.metadata_response:
            table = next(t for t in tables if t.get("name") == mock_table["name"])
            self.assertEqual(len(table["fields"]), len(mock_table["fields"]))
            for mock_field in mock_table["fields"]:
                field = next(
                    f for f in table["fields"] if f.get("name") == mock_field["name"]
                )
                self.assertEqual(field.get("type"), mock_field["type"])

        # Verify that the metadata was cached and not re-fetched for fetch_columns
        self.metadata_endpoint.assert_called_once()

    def test_sql_query_execution(self):
        sql_query = "SELECT Name, Status, CreatedDate FROM Account LIMIT 10"
        commands = [
            {"method": "cursor", "store": "_cursor"},
            {"args": [sql_query], "method": "execute", "target": "_cursor"},
            {"method": "fetchall", "store": "tmp_1", "target": "_cursor"},
            {"method": "description", "store": "tmp_2", "target": "_cursor"},
            {"method": "close", "target": "_cursor"},
            {
                "kwargs": {
                    "all_results": {"__reference__": "tmp_1"},
                    "description": {"__reference__": "tmp_2"},
                },
                "method": "build_dict",
                "target": "__utils",
            },
        ]
        operation = {
            "commands": commands,
            "skip_cache": True,
            "trace_id": "f6e0e3fe-e03c-4f6f-9bfd-55478350ea45",
        }

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_list_tables",
            operation_dict=operation,
            credentials=self.credentials,
        )

        self.assertFalse(response.is_error)
        result = response.result[ATTRIBUTE_NAME_RESULT]

        for i, row in enumerate(self.data_response["data"]):
            self.assertEqual(result["all_results"][i][0], row[0])
            self.assertEqual(result["all_results"][i][1], row[1])

        for i, (key, value) in enumerate(self.data_response["metadata"].items()):
            self.assertEqual(result["description"][i][0], key)
            self.assertEqual(result["description"][i][1], value["type"])

    def test_list_tables_with_invalid_dataspace_raises_clear_error(self):
        """
        When the dataspace token exchange fails (e.g. dataspace doesn't exist), the error should
        be clear rather than "Token Renewal failed with code 400" from the fake refresh_token.
        """
        # Make the a360/token endpoint fail for this test
        self.mock_responses.remove(
            responses.POST, "https://test.salesforce.com/services/a360/token"
        )
        self.mock_responses.add(
            method=responses.POST,
            url="https://test.salesforce.com/services/a360/token",
            status=400,
            body=json.dumps({"error": "invalid_dataspace"}),
        )

        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,
            "commands": [
                {
                    "method": "list_tables",
                    "kwargs": {"dataspace": "NonExistentDataspace"},
                }
            ],
        }

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_list_tables_invalid_dataspace",
            operation_dict=operation,
            credentials=self.credentials,
        )

        self.assertTrue(response.is_error)
        error_message = str(response.result)
        # Should NOT see the misleading "Token Renewal failed" error
        self.assertNotIn("Token Renewal failed", error_message)
        # Should see a clear token exchange error
        self.assertIn("Token exchange failed", error_message)

    def test_list_tables_with_invalid_dataspace_raises_clear_error_clean_path(self):
        """
        Older versions of salesforce-cdp-connector raise KeyError('access_token') when the
        a360/token exchange fails (instead of a typed Error). Verify this is wrapped into a
        readable RuntimeError rather than surfacing as AgentClientError: 'access_token'.
        """
        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,
            "commands": [
                {
                    "method": "list_tables",
                    "kwargs": {"dataspace": "NonExistentDataspace"},
                }
            ],
        }

        # Use clean-credentials path: no core_token
        del self.credentials["connect_args"]["core_token"]

        # Simulate the older salesforce-cdp-connector behavior that raises KeyError
        # instead of a typed Error when the a360 exchange fails.
        with patch(
            "salesforcecdpconnector.connection.SalesforceCDPConnection.list_tables",
            side_effect=KeyError("access_token"),
        ):
            response = self.agent.execute_operation(
                connection_type="salesforce-data-cloud",
                operation_name="test_list_tables_invalid_dataspace_clean",
                operation_dict=operation,
                credentials=self.credentials,
            )

        self.assertTrue(response.is_error)
        error_message = str(response.result)
        # Should NOT see the raw KeyError: 'access_token'
        self.assertNotIn("KeyError", error_message)
        # Should see a clear token exchange error mentioning the dataspace
        self.assertIn("Token exchange failed", error_message)
        self.assertIn("NonExistentDataspace", error_message)

    def test_list_tables_invalid_dataspace_surfaces_http_status_code(self):
        """
        When the a360/token exchange returns a non-200 response, the error message must
        include the HTTP status code (from SalesforceCDPError) so the caller can distinguish
        between auth failures (401/403) and bad dataspace names (400/404).
        """
        self.mock_responses.remove(
            responses.POST, "https://test.salesforce.com/services/a360/token"
        )
        self.mock_responses.add(
            method=responses.POST,
            url="https://test.salesforce.com/services/a360/token",
            status=403,
            body=json.dumps(
                {
                    "error": "insufficient_scope",
                    "error_description": "Run-As user lacks access",
                }
            ),
        )

        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,
            "commands": [
                {
                    "method": "list_tables",
                    "kwargs": {"dataspace": "UnifiedKnowledge"},
                }
            ],
        }

        # Use clean-credentials path (no core_token) so the per-dataspace connection
        # goes through _token_by_client_creds_flow and raises SalesforceCDPError.
        credentials = {**self.credentials}
        credentials["connect_args"] = {
            k: v
            for k, v in self.credentials["connect_args"].items()
            if k != "core_token"
        }

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_list_tables_http_status",
            operation_dict=operation,
            credentials=credentials,
        )

        self.assertTrue(response.is_error)
        error_message = str(response.result)
        # Status code from SalesforceCDPError must be surfaced
        self.assertIn("403", error_message)
        # Must still say "Token exchange failed"
        self.assertIn("Token exchange failed", error_message)
        # Hint about Run-As user / dataspace name should be present
        self.assertIn("Run-As user", error_message)
        # Salesforce response body must be included so it reaches Datadog via data-collector
        self.assertIn("insufficient_scope", error_message)

    def test_capturing_session_stores_body_and_status(self):
        """
        _attach_capturing_session stores last_exchange_body and last_exchange_status on
        the _CapturingSession regardless of response status code (including non-200).
        This validates that the plumbing is in place so error handlers can include the
        captured body in RuntimeErrors propagated to the data-collector and Datadog.
        """
        from apollo.integrations.db.salesforce_data_cloud_proxy_client import (
            SalesforceDataCloudConnection,
            _attach_capturing_session,
        )

        self.mock_responses.remove(
            responses.POST, "https://test.salesforce.com/services/a360/token"
        )
        self.mock_responses.add(
            method=responses.POST,
            url="https://test.salesforce.com/services/a360/token",
            status=400,
            body=json.dumps({"error": "dataspace_not_found"}),
        )

        conn = SalesforceDataCloudConnection(
            "https://test.salesforce.com",
            client_id="test_client_id",
            client_secret="test_client_secret",
            core_token=None,
            refresh_token=None,
            dataspace="BadDataspace",
        )
        capturing = _attach_capturing_session(conn)
        self.assertIsNotNone(capturing)

        try:
            conn.list_tables()
        except Exception:
            pass

        # The capturing session must have stored the response body and status
        self.assertIsNotNone(capturing.last_exchange_body)
        self.assertIsNotNone(capturing.last_exchange_status)
        self.assertIn("dataspace_not_found", capturing.last_exchange_body)
        self.assertEqual(capturing.last_exchange_status, 400)

    def test_list_tables_keyerror_includes_response_body(self):
        """
        When Salesforce returns HTTP 200 but with a body missing 'access_token'
        (a 200-with-error-payload pattern observed in the wild), the KeyError path
        must still include the captured body and status in the error message.
        """
        self.mock_responses.remove(
            responses.POST, "https://test.salesforce.com/services/a360/token"
        )
        # Salesforce returns 200 but with an unexpected body (no access_token)
        self.mock_responses.add(
            method=responses.POST,
            url="https://test.salesforce.com/services/a360/token",
            status=200,
            body=json.dumps(
                {"error": "invalid_dataspace", "message": "Dataspace not found"}
            ),
        )

        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,
            "commands": [
                {
                    "method": "list_tables",
                    "kwargs": {"dataspace": "UnifiedKnowledge"},
                }
            ],
        }

        credentials = {**self.credentials}
        credentials["connect_args"] = {
            k: v
            for k, v in self.credentials["connect_args"].items()
            if k != "core_token"
        }

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_list_tables_keyerror_body",
            operation_dict=operation,
            credentials=credentials,
        )

        self.assertTrue(response.is_error)
        error_message = str(response.result)
        self.assertIn("Token exchange failed", error_message)
        # HTTP status and body must both appear in the error message
        self.assertIn("HTTP 200", error_message)
        self.assertIn("invalid_dataspace", error_message)

    def test_classify_exchange_status(self):
        """_classify_exchange_status returns the right label for each HTTP status family."""
        from apollo.integrations.db.salesforce_data_cloud_proxy_client import (
            _classify_exchange_status,
        )

        self.assertEqual(_classify_exchange_status(429), "rate_limited")
        self.assertEqual(_classify_exchange_status(401), "auth_failed")
        self.assertEqual(_classify_exchange_status(403), "auth_failed")
        self.assertEqual(_classify_exchange_status(400), "bad_request")
        self.assertEqual(_classify_exchange_status(500), "server_error")
        self.assertEqual(_classify_exchange_status(503), "server_error")
        self.assertEqual(_classify_exchange_status(200), "other")
        self.assertEqual(_classify_exchange_status(None), "unknown")

    def test_warning_logged_with_status_code_on_cdp_error(self):
        """
        When SalesforceCDPError is raised (non-200 a360/token response), logger.warning
        must be called with exchange_status_code and exchange_error_type as structured fields.
        This ensures throttling (429) is distinguishable from auth failures (403) in logs.
        """
        from unittest.mock import patch
        from apollo.integrations.db.salesforce_data_cloud_proxy_client import (
            SalesforceDataCloudProxyClient,
            SalesforceDataCloudCredentials,
        )

        # a360/token returns 429 (throttled)
        self.mock_responses.remove(
            responses.POST, "https://test.salesforce.com/services/a360/token"
        )
        self.mock_responses.add(
            method=responses.POST,
            url="https://test.salesforce.com/services/a360/token",
            status=429,
            body=json.dumps({"error": "rate_limit_exceeded"}),
        )

        client = SalesforceDataCloudProxyClient(
            SalesforceDataCloudCredentials(
                domain="test.salesforce.com",
                client_id="test_client_id",
                client_secret="test_client_secret",
                core_token=None,
                refresh_token=None,
            )
        )

        with patch(
            "apollo.integrations.db.salesforce_data_cloud_proxy_client.logger"
        ) as mock_logger:
            with self.assertRaises(RuntimeError):
                client.list_tables(dataspace="UnifiedKnowledge")

        mock_logger.warning.assert_called_once()
        extra = mock_logger.warning.call_args.kwargs.get("extra", {})
        self.assertEqual(extra.get("exchange_status_code"), 429)
        self.assertEqual(extra.get("exchange_error_type"), "rate_limited")
        self.assertEqual(extra.get("dataspace"), "UnifiedKnowledge")
        # Response body must be present as a structured field (redacted of any tokens)
        self.assertIsNotNone(extra.get("exchange_response_body"))
        self.assertIn("rate_limit_exceeded", extra.get("exchange_response_body", ""))

    def test_warning_logged_with_missing_access_token_type_on_keyerror(self):
        """
        When Salesforce returns HTTP 200 but with a body missing 'access_token' (KeyError path),
        logger.warning must be called with exchange_error_type='missing_access_token' so the
        200-with-error pattern is distinguishable from non-200 failures in logs.
        """
        from unittest.mock import patch
        from apollo.integrations.db.salesforce_data_cloud_proxy_client import (
            SalesforceDataCloudProxyClient,
            SalesforceDataCloudCredentials,
        )

        # Salesforce returns 200 but with a body that has no access_token
        self.mock_responses.remove(
            responses.POST, "https://test.salesforce.com/services/a360/token"
        )
        self.mock_responses.add(
            method=responses.POST,
            url="https://test.salesforce.com/services/a360/token",
            status=200,
            body=json.dumps(
                {"error": "invalid_dataspace", "message": "Dataspace not found"}
            ),
        )

        client = SalesforceDataCloudProxyClient(
            SalesforceDataCloudCredentials(
                domain="test.salesforce.com",
                client_id="test_client_id",
                client_secret="test_client_secret",
                core_token=None,
                refresh_token=None,
            )
        )

        with patch(
            "apollo.integrations.db.salesforce_data_cloud_proxy_client.logger"
        ) as mock_logger:
            with self.assertRaises(RuntimeError):
                client.list_tables(dataspace="UnifiedKnowledge")

        mock_logger.warning.assert_called_once()
        extra = mock_logger.warning.call_args.kwargs.get("extra", {})
        self.assertEqual(extra.get("exchange_error_type"), "missing_access_token")
        self.assertEqual(extra.get("exchange_status_code"), 200)
        self.assertEqual(extra.get("dataspace"), "UnifiedKnowledge")
        # Response body must be present as a structured field
        self.assertIsNotNone(extra.get("exchange_response_body"))
        self.assertIn("invalid_dataspace", extra.get("exchange_response_body", ""))

    def test_any_keyerror_is_wrapped_with_structured_logging(self):
        """
        Any KeyError from conn.list_tables() — not just KeyError('access_token') —
        is caught, logged with structured fields (exchange_status_code, exchange_error_type,
        exchange_response_body), and wrapped in a clear RuntimeError.

        This ensures that if the library raises KeyError for other missing fields
        (e.g. 'instance_url', 'token_type') we still get full diagnostic context
        in Datadog rather than a raw KeyError propagating to the caller.
        """
        from unittest.mock import patch
        from apollo.integrations.db.salesforce_data_cloud_proxy_client import (
            SalesforceDataCloudProxyClient,
            SalesforceDataCloudCredentials,
        )

        client = SalesforceDataCloudProxyClient(
            SalesforceDataCloudCredentials(
                domain="test.salesforce.com",
                client_id="test_client_id",
                client_secret="test_client_secret",
                core_token=None,
                refresh_token=None,
            )
        )

        with patch(
            "salesforcecdpconnector.connection.SalesforceCDPConnection.list_tables",
            side_effect=KeyError("instance_url"),
        ):
            with patch(
                "apollo.integrations.db.salesforce_data_cloud_proxy_client.logger"
            ) as mock_logger:
                with self.assertRaises(RuntimeError) as ctx:
                    client.list_tables(dataspace="UnifiedKnowledge")

        # Must be wrapped as a clear RuntimeError, not a raw KeyError
        self.assertIn("Token exchange failed", str(ctx.exception))
        self.assertIn("instance_url", str(ctx.exception))
        # Structured warning must be emitted
        mock_logger.warning.assert_called_once()
        extra = mock_logger.warning.call_args.kwargs.get("extra", {})
        self.assertEqual(extra.get("exchange_error_type"), "missing_access_token")
        self.assertEqual(extra.get("dataspace"), "UnifiedKnowledge")

    def test_access_token_redacted_from_error_on_successful_exchange(self):
        """
        If the a360/token exchange SUCCEEDS (body contains access_token) but a KeyError
        fires later for an unrelated reason, the captured body is redacted before being
        included in any error so the real token is never exposed.
        """
        from apollo.integrations.db.salesforce_data_cloud_proxy_client import (
            _redact_body,
        )

        body_with_token = "{'access_token': 'eyJREAL_SECRET_TOKEN', 'expires_in': 3600}"
        redacted = _redact_body(body_with_token)
        self.assertIsNotNone(redacted)
        self.assertNotIn("eyJREAL_SECRET_TOKEN", redacted)
        self.assertIn("[REDACTED]", redacted)
        # Non-sensitive fields must still be present
        self.assertIn("expires_in", redacted)
