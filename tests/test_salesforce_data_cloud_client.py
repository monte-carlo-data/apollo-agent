import collections
import http.client
import json
import threading
import urllib.parse
import uuid
from typing import Any
from unittest import TestCase
from unittest.mock import Mock, patch

import requests
import responses
import urllib3.exceptions

from apollo.agent.agent import Agent
from apollo.common.agent.constants import ATTRIBUTE_NAME_RESULT
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.db import salesforce_data_cloud_proxy_client as sfdc_module
from apollo.integrations.db.salesforce_data_cloud_proxy_client import (
    _CORE_TOKEN_CACHE,
    _DISCOVERY_REQUEST_TIMEOUT_SECONDS,
    _SSOT_REQUEST_TIMEOUT_DEFAULT_SECONDS,
    _SSOT_REQUEST_TIMEOUT_MAX_SECONDS,
    _CapturingSession,
    _is_expired_session,
    _retry_read_timeout,
    _RetryingSalesforceDataCloudCursor,
    SalesforceDataCloudCredentials,
    SalesforceDataCloudProxyClient,
)


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

        # The core-token cache (YET-2522) is process-wide module state; clear it
        # so tokens minted in one test never leak into another (each test's mint
        # mock returns a fresh uuid, so a leaked token would be a stale value that
        # silently suppresses the mint a test expects).
        _CORE_TOKEN_CACHE.clear()
        self.addCleanup(_CORE_TOKEN_CACHE.clear)

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

    def test_query_token_exchange_failure_surfaces_status_and_body(self):
        """
        Old-DC-path query connections (core_token present) must surface the captured
        a360/token exchange HTTP status and response body instead of only the generic
        "Token exchange failed..." message (YET-1790). The library swallows the
        exchange failure (``except Exception: pass``) before falling into
        ``_renew_token``, so the detail must come from the ``_CapturingSession``
        attached to the connection's authentication_helper.
        """
        self.mock_responses.remove(
            responses.POST, "https://test.salesforce.com/services/a360/token"
        )
        self.mock_responses.add(
            method=responses.POST,
            url="https://test.salesforce.com/services/a360/token",
            status=400,
            body=json.dumps(
                {"error": "invalid_request", "error_description": "dataspace not found"}
            ),
        )
        # Model the size-collection dispatch: dataspace-scoped query credentials.
        self.credentials["connect_args"]["dataspace"] = "csg"

        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,
            "commands": [
                {"method": "cursor", "store": "_cursor"},
                {
                    "args": ["SELECT COUNT(*) FROM t__dll"],
                    "method": "execute",
                    "target": "_cursor",
                },
            ],
        }

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_query_token_exchange_failure",
            operation_dict=operation,
            credentials=self.credentials,
        )

        self.assertTrue(response.is_error)
        msg = str(response.result)
        self.assertIn("Token exchange failed", msg)
        self.assertIn("HTTP 400", msg)
        self.assertIn("dataspace not found", msg)

    def test_query_token_exchange_failure_without_response_stays_generic(self):
        """
        When the a360 exchange fails without an HTTP response (e.g. the connection
        drops), there is nothing to capture — the query-path error must stay the
        generic message without a bogus "HTTP None" suffix.
        """
        self.mock_responses.remove(
            responses.POST, "https://test.salesforce.com/services/a360/token"
        )
        self.mock_responses.add(
            method=responses.POST,
            url="https://test.salesforce.com/services/a360/token",
            body=requests.exceptions.ConnectionError("connection dropped"),
        )

        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,
            "commands": [
                {"method": "cursor", "store": "_cursor"},
                {
                    "args": ["SELECT COUNT(*) FROM t__dll"],
                    "method": "execute",
                    "target": "_cursor",
                },
            ],
        }

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_query_token_exchange_failure_no_response",
            operation_dict=operation,
            credentials=self.credentials,
        )

        self.assertTrue(response.is_error)
        msg = str(response.result)
        self.assertIn("Token exchange failed", msg)
        self.assertNotIn("HTTP", msg)
        self.assertNotIn("Salesforce response", msg)

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

    def test_list_tables_metadata_query_failure_not_labeled_token_exchange(self):
        """A post-token-exchange getAllMetadata failure (e.g. a transient DEADLINE_EXCEEDED) is
        reported as a metadata-query error, NOT 'Token exchange failed ... verify permission'
        (YET-1631) — the token exchange succeeded; only the metadata query failed."""
        from salesforcecdpconnector.exceptions import Error as SalesforceCDPError

        metadata_err = SalesforceCDPError(
            "Failed executing metadata query in server : status=500, message="
            "TranslationMetadataHelper.getAllMetadata Error fetching data source entities - "
            "DEADLINE_EXCEEDED: CallOptions deadline exceeded after 9.9s"
        )
        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,
            "commands": [{"method": "list_tables", "kwargs": {"dataspace": "default"}}],
        }
        with patch(
            "apollo.integrations.db.salesforce_data_cloud_proxy_client."
            "SalesforceDataCloudConnection.list_tables",
            side_effect=metadata_err,
        ):
            response = self.agent.execute_operation(
                connection_type="salesforce-data-cloud",
                operation_name="test_metadata_query_failure",
                operation_dict=operation,
                credentials=self.credentials,
            )

        self.assertTrue(response.is_error)
        msg = str(response.result)
        self.assertIn("Metadata query failed for dataspace 'default'", msg)
        self.assertNotIn("Token exchange failed", msg)
        self.assertNotIn("verify the dataspace name", msg)

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

    def test_client_credentials_flow_does_not_revoke_core_token(self):
        """YET-1546: on the client-credentials (no core_token) path, the freshly minted
        core token must NOT be revoked after the a360 exchange. Salesforce reuses one
        platform session per connected app, so revoking it invalidates the session the
        data-collector reuses for its /ssot/* + SOAP metadata calls (INVALID_SESSION_ID).
        """
        revoke_callback = Mock(return_value=(200, {}, ""))
        self.mock_responses.remove(
            responses.POST, "https://test.salesforce.com/services/oauth2/revoke"
        )
        self.mock_responses.add_callback(
            method=responses.POST,
            url="https://test.salesforce.com/services/oauth2/revoke",
            callback=revoke_callback,
        )

        operation = {
            "trace_id": "test-trace-id",
            "skip_cache": True,
            "commands": [
                {"method": "list_tables", "kwargs": {"dataspace": "UnifiedKnowledge"}}
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
            operation_name="test_no_revoke_client_creds",
            operation_dict=operation,
            credentials=credentials,
        )

        self.assertFalse(response.is_error)
        revoke_callback.assert_not_called()

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

    def test_query_connection_scoped_to_dataspace(self):
        """
        When `dataspace` is included in connect_args, the a360/token exchange must include
        it as a query parameter so queries against tables in non-default dataspaces succeed.

        Without this, the token is scoped to the base tenant and Salesforce returns:
          NOT_FOUND: DataSourceEntity with developerName = <table> and tenantId = a360/prod/<id> is not found
        """
        from urllib.parse import urlparse, parse_qs

        a360_requests = []

        def capturing_a360_endpoint(request):
            a360_requests.append(request)
            return (
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

        self.mock_responses.remove(
            responses.POST, "https://test.salesforce.com/services/a360/token"
        )
        self.mock_responses.add_callback(
            method=responses.POST,
            url="https://test.salesforce.com/services/a360/token",
            callback=capturing_a360_endpoint,
        )

        credentials = {
            "connect_args": {
                **self.credentials["connect_args"],
                "dataspace": "unified_knowledge",
            }
        }

        sql_query = "SELECT Id FROM abc_fit_tests__dll LIMIT 1"
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
            "trace_id": "test-dataspace-scoped-query",
        }

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_query_scoped",
            operation_dict=operation,
            credentials=credentials,
        )

        self.assertFalse(response.is_error)

        # The a360/token POST must have been called with dataspace=unified_knowledge
        self.assertGreater(
            len(a360_requests), 0, "Expected at least one a360/token call"
        )
        a360_request = a360_requests[0]
        query_params = parse_qs(urlparse(a360_request.url).query)
        self.assertEqual(
            query_params.get("dataspace"),
            ["unified_knowledge"],
            "a360/token POST must include dataspace=unified_knowledge query param",
        )

    def test_query_connection_unscoped_when_no_dataspace(self):
        """
        When `dataspace` is absent from connect_args (default / legacy path), the a360/token
        exchange must NOT include a dataspace param — existing customers are unaffected.
        """
        from urllib.parse import urlparse, parse_qs

        a360_requests = []

        def capturing_a360_endpoint(request):
            a360_requests.append(request)
            return (
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

        self.mock_responses.remove(
            responses.POST, "https://test.salesforce.com/services/a360/token"
        )
        self.mock_responses.add_callback(
            method=responses.POST,
            url="https://test.salesforce.com/services/a360/token",
            callback=capturing_a360_endpoint,
        )

        # Use default credentials — no dataspace field
        sql_query = "SELECT Id FROM Account LIMIT 1"
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
            "trace_id": "test-unscoped-query",
        }

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_query_unscoped",
            operation_dict=operation,
            credentials=self.credentials,
        )

        self.assertFalse(response.is_error)

        # The a360/token POST must NOT include a dataspace param
        self.assertGreater(
            len(a360_requests), 0, "Expected at least one a360/token call"
        )
        a360_request = a360_requests[0]
        query_params = parse_qs(urlparse(a360_request.url).query)
        self.assertNotIn(
            "dataspace",
            query_params,
            "Unscoped path must not include dataspace in a360/token POST",
        )

    def test_list_tables_unscoped_even_when_client_has_dataspace(self):
        """
        When list_tables(dataspace=None) is called on a proxy client that was
        instantiated with a dataspace (i.e. a query-execution client), the
        a360/token exchange must NOT include a dataspace param.  Without this
        guard a future caller could accidentally receive dataspace-scoped table
        results while believing the fetch was unscoped.
        """
        from urllib.parse import urlparse, parse_qs

        a360_requests = []

        def capturing_a360_endpoint(request):
            a360_requests.append(request)
            return (
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

        self.mock_responses.remove(
            responses.POST, "https://test.salesforce.com/services/a360/token"
        )
        self.mock_responses.add_callback(
            method=responses.POST,
            url="https://test.salesforce.com/services/a360/token",
            callback=capturing_a360_endpoint,
        )

        # Credentials include a dataspace (as injected by the monolith for query jobs)
        scoped_credentials = {
            "connect_args": {
                "domain": "test.salesforce.com",
                "client_id": "test_client_id",
                "client_secret": "test_client_secret",
                "core_token": "test_core_token",
                "dataspace": "unified_knowledge",
            }
        }

        commands = [
            {
                "method": "list_tables",
                "store": "tables",
                "kwargs": {"dataspace": None},
            },
        ]
        operation = {
            "commands": commands,
            "skip_cache": True,
            "trace_id": "test-list-tables-unscoped-on-scoped-client",
        }

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_list_tables_unscoped_on_scoped_client",
            operation_dict=operation,
            credentials=scoped_credentials,
        )

        self.assertFalse(response.is_error)

        # Even though the client was created with dataspace=unified_knowledge,
        # the list_tables(None) call must use an unscoped a360/token exchange
        self.assertGreater(
            len(a360_requests), 0, "Expected at least one a360/token call"
        )
        for req in a360_requests:
            query_params = parse_qs(urlparse(req.url).query)
            self.assertNotIn(
                "dataspace",
                query_params,
                "list_tables(None) on a scoped client must not include dataspace in a360/token POST",
            )

    # -- Dataspace auto-discovery via SOQL (YET-1256) ---------------------------------

    _PROXY_CLIENT_LOGGER = "apollo.integrations.db.salesforce_data_cloud_proxy_client"
    _SOQL_QUERY_URL = "https://test.salesforce.com/services/data/v62.0/query"

    def _add_soql_callback(self, callback):
        """Register a callback for the SOQL dataspace discovery endpoint."""
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SOQL_QUERY_URL,
            callback=callback,
        )

    def _list_dataspaces_operation(self) -> dict:
        return {
            "trace_id": "test-trace-id",
            "skip_cache": True,
            "commands": [{"method": "list_dataspaces"}],
        }

    def test_list_dataspaces(self):
        """Success: SOQL returns N dataspaces and `list_dataspaces` returns the API names."""
        discovered = ["default", "CSG", "Unified_Knowledge", "Professional_Services"]
        self._add_soql_callback(
            Mock(
                return_value=(
                    200,
                    {},
                    json.dumps(
                        {
                            "records": [{"DataSpaceApiName": ds} for ds in discovered],
                            "done": True,
                        }
                    ),
                )
            )
        )

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_list_dataspaces",
            operation_dict=self._list_dataspaces_operation(),
            credentials=self.credentials,
        )

        self.assertFalse(response.is_error, msg=str(response.result))
        self.assertEqual(response.result[ATTRIBUTE_NAME_RESULT], discovered)

    def test_list_dataspaces_empty(self):
        """An empty `records` array returns an empty result. The caller (data-collector)
        decides whether that's a permission gap worth surfacing — the agent just reports it.

        The agent's RPC layer serializes Python `[]` to `{}` over the wire, so this test
        asserts the result is falsy rather than pinning a specific representation."""
        self._add_soql_callback(
            Mock(return_value=(200, {}, json.dumps({"records": [], "done": True})))
        )

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_list_dataspaces_empty",
            operation_dict=self._list_dataspaces_operation(),
            credentials=self.credentials,
        )

        self.assertFalse(response.is_error, msg=str(response.result))
        self.assertFalse(
            response.result[ATTRIBUTE_NAME_RESULT],
            msg=f"expected empty result, got {response.result[ATTRIBUTE_NAME_RESULT]!r}",
        )

    def test_list_dataspaces_paginates_via_next_records_url(self):
        """`done=False` with `nextRecordsUrl` triggers a follow-up GET; results accumulate."""
        page_1 = {
            "records": [
                {"DataSpaceApiName": "page1_ds1"},
                {"DataSpaceApiName": "page1_ds2"},
            ],
            "done": False,
            "nextRecordsUrl": "/services/data/v62.0/query/01gXX-page2",
        }
        page_2 = {
            "records": [{"DataSpaceApiName": "page2_ds1"}],
            "done": True,
        }

        first_page_url = self._SOQL_QUERY_URL
        next_page_url = (
            "https://test.salesforce.com/services/data/v62.0/query/01gXX-page2"
        )

        self.mock_responses.add_callback(
            method=responses.GET,
            url=first_page_url,
            callback=Mock(return_value=(200, {}, json.dumps(page_1))),
        )
        self.mock_responses.add_callback(
            method=responses.GET,
            url=next_page_url,
            callback=Mock(return_value=(200, {}, json.dumps(page_2))),
        )

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_list_dataspaces_paginates",
            operation_dict=self._list_dataspaces_operation(),
            credentials=self.credentials,
        )

        self.assertFalse(response.is_error, msg=str(response.result))
        self.assertEqual(
            response.result[ATTRIBUTE_NAME_RESULT],
            ["page1_ds1", "page1_ds2", "page2_ds1"],
        )

    def test_list_dataspaces_soql_http_error_surfaces_status(self):
        """A non-200 from the SOQL endpoint propagates as a RuntimeError with `code NNN`
        in the message so the data-collector's `_extract_exchange_http_status` can pick it up.
        """
        self._add_soql_callback(
            Mock(
                return_value=(
                    401,
                    {},
                    json.dumps(
                        [
                            {
                                "errorCode": "INVALID_SESSION_ID",
                                "message": "Session expired",
                            }
                        ]
                    ),
                )
            )
        )

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_list_dataspaces_soql_http_error",
            operation_dict=self._list_dataspaces_operation(),
            credentials=self.credentials,
        )

        self.assertTrue(response.is_error)
        message = str(response.result)
        self.assertIn("code 401", message)
        self.assertIn("INVALID_SESSION_ID", message)

    def test_list_dataspaces_oauth_http_error_redacts_body(self):
        """A non-200 from the OAuth token mint propagates as a RuntimeError with
        the response body redacted — no raw `access_token` string in the exception."""
        leaked_token = "secret-token-should-be-redacted"
        leaked_body = json.dumps(
            {"error": "invalid_client", "access_token": leaked_token}
        )

        self.mock_responses.remove(
            responses.POST, "https://test.salesforce.com/services/oauth2/token"
        )
        self.mock_responses.add_callback(
            method=responses.POST,
            url="https://test.salesforce.com/services/oauth2/token",
            callback=Mock(return_value=(401, {}, leaked_body)),
        )

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_list_dataspaces_oauth_http_error",
            operation_dict=self._list_dataspaces_operation(),
            credentials=self.credentials,
        )

        self.assertTrue(response.is_error)
        message = str(response.result)
        self.assertIn("code 401", message)
        self.assertIn("[REDACTED]", message)
        self.assertNotIn(leaked_token, message)

    def test_list_dataspaces_rate_limit_logs_classification(self):
        """A 429 surfaces as a RuntimeError AND the structured log record carries
        `exchange_error_type=rate_limited` so Datadog queries (e.g.
        `@exchange_error_type:rate_limited`) catch it."""
        self._add_soql_callback(
            Mock(
                return_value=(
                    429,
                    {},
                    json.dumps(
                        [{"errorCode": "REQUEST_LIMIT_EXCEEDED", "message": "Too many"}]
                    ),
                )
            )
        )

        with self.assertLogs(self._PROXY_CLIENT_LOGGER, level="WARNING") as logs:
            response = self.agent.execute_operation(
                connection_type="salesforce-data-cloud",
                operation_name="test_list_dataspaces_rate_limit",
                operation_dict=self._list_dataspaces_operation(),
                credentials=self.credentials,
            )

        self.assertTrue(response.is_error)
        self.assertIn("code 429", str(response.result))
        rate_limited_records = [
            r
            for r in logs.records
            if getattr(r, "exchange_error_type", None) == "rate_limited"
        ]
        self.assertTrue(
            rate_limited_records,
            f"expected a rate_limited classification in logs; got {logs.output}",
        )

    # -- Generic SSOT reads (YET-1615) -----------------------------------------------

    _SSOT_PATH = "/services/data/v62.0/ssot/data-streams"
    _SSOT_URL = "https://test.salesforce.com/services/data/v62.0/ssot/data-streams"

    def _ssot_get_operation(self, path: str, **extra_kwargs: Any) -> dict:
        kwargs: dict = {"path": path}
        kwargs.update(extra_kwargs)
        return {
            "trace_id": "test-trace-id",
            "skip_cache": True,
            "commands": [{"method": "ssot_get", "kwargs": kwargs}],
        }

    def test_ssot_get_returns_json_and_uses_minted_core_token(self):
        """ssot_get mints a client-credentials core token and GETs the My Domain
        core REST path with it as a Bearer credential, returning the parsed JSON.
        The minted token is never returned to the caller (the data-collector)."""
        ssot_body = {
            "dataStreams": [
                {"name": "Web_Engagement", "totalRecords": 42},
                {"name": "Email_Engagement", "totalRecords": 7},
            ],
            "totalSize": 2,
        }
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(return_value=(200, {}, json.dumps(ssot_body))),
        )

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_ssot_get",
            operation_dict=self._ssot_get_operation(self._SSOT_PATH),
            credentials=self.credentials,
        )

        self.assertFalse(response.is_error, msg=str(response.result))
        self.assertEqual(response.result[ATTRIBUTE_NAME_RESULT], ssot_body)

        # A core token was minted via the client-credentials grant.
        self.client_credentials_token_endpoint.assert_called()

        # The /ssot GET carried the minted core token as a Bearer credential ...
        ssot_call = next(
            c
            for c in self.mock_responses.calls
            if "/ssot/data-streams" in c.request.url
        )
        self.assertEqual(
            ssot_call.request.headers["Authorization"],
            f"Bearer {self.client_credentials_token}",
        )
        # ... and the token never leaks back to the caller in the result.
        self.assertNotIn(self.client_credentials_token, json.dumps(response.result))

    def test_ssot_get_resource_with_description_string_passes_through(self):
        """A per-resource /ssot body carrying a top-level human-readable
        `description` STRING (e.g. a Data 360 Retriever detail, YET-2410) must be
        returned verbatim. BaseDbProxyClient.process_result used to mistake any
        dict with a `description` key for a DB-API cursor result and index each
        character of the string, failing EVERY such op with
        `IndexError: string index out of range`."""
        detail_path = "/services/data/v62.0/ssot/machine-learning/retrievers/sfdc_ai__WebRetrievalAction"
        detail_body = {
            "name": "WebRetrievalAction",
            "namespace": "sfdc_ai",
            "description": "Web search retriever retrieves search results from the internet.",
            "activeConfiguration": {
                "name": "WebRetrievalActionVersion",
                "isActive": True,
            },
        }
        self.mock_responses.add_callback(
            method=responses.GET,
            url=f"https://test.salesforce.com{detail_path}",
            callback=Mock(return_value=(200, {}, json.dumps(detail_body))),
        )

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_ssot_get_retriever_detail",
            operation_dict=self._ssot_get_operation(detail_path),
            credentials=self.credentials,
        )

        self.assertFalse(response.is_error, msg=str(response.result))
        self.assertEqual(response.result[ATTRIBUTE_NAME_RESULT], detail_body)

    def test_ssot_get_rejects_absolute_url(self):
        """ssot_get must only ever target the connection's own My Domain. An
        absolute URL (or any value carrying a scheme/host) is rejected before a
        token is minted, so the customer's credential is never sent elsewhere."""
        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_ssot_get_absolute_url",
            operation_dict=self._ssot_get_operation("https://evil.example.com/steal"),
            credentials=self.credentials,
        )

        self.assertTrue(response.is_error)
        # The guard fired before any network call (a network attempt would surface
        # a connection error, not this message).
        self.assertIn("must be a relative path", str(response.result))

    def test_ssot_get_rejects_protocol_relative_url(self):
        """A protocol-relative value (`//host/...`) is also rejected — it would
        otherwise resolve to a different host."""
        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_ssot_get_protocol_relative",
            operation_dict=self._ssot_get_operation("//evil.example.com/steal"),
            credentials=self.credentials,
        )

        self.assertTrue(response.is_error)
        self.assertIn("must be a relative path", str(response.result))

    def test_ssot_get_http_error_surfaces_status_and_redacts_body(self):
        """A non-200 from the SSOT endpoint surfaces as a RuntimeError carrying
        `code NNN` (so the data-collector can extract the HTTP status), with the
        Salesforce error body included and any access_token redacted."""
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(
                return_value=(
                    404,
                    {},
                    json.dumps(
                        [{"errorCode": "NOT_FOUND", "message": "no such resource"}]
                    ),
                )
            ),
        )

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_ssot_get_http_error",
            operation_dict=self._ssot_get_operation(self._SSOT_PATH),
            credentials=self.credentials,
        )

        self.assertTrue(response.is_error)
        message = str(response.result)
        self.assertIn("code 404", message)
        self.assertIn("NOT_FOUND", message)

    def test_ssot_get_forwards_query_string(self):
        """A path carrying a query string (`?limit=100`) is allowed by the guard
        and forwarded to Salesforce verbatim — `responses` matches on the base
        URL, so assert the actual request URL preserved the query string."""
        path_with_query = "/services/data/v62.0/ssot/data-streams?limit=100"
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(return_value=(200, {}, json.dumps({"dataStreams": []}))),
        )

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_ssot_get_query_string",
            operation_dict=self._ssot_get_operation(path_with_query),
            credentials=self.credentials,
        )

        self.assertFalse(response.is_error, msg=str(response.result))
        ssot_call = next(
            c
            for c in self.mock_responses.calls
            if "/ssot/data-streams" in c.request.url
        )
        self.assertIn("limit=100", ssot_call.request.url)

    def test_ssot_get_allows_url_valued_query_param(self):
        """The path guard must reject values by their URL *structure* (scheme or
        host present), not by substring — a relative path whose query string
        merely embeds a URL (e.g. a pagination cursor) is safe and allowed."""
        path_with_url_cursor = (
            "/services/data/v62.0/ssot/data-streams"
            "?next=https://api.salesforce.com/cursor/abc"
        )
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(return_value=(200, {}, json.dumps({"dataStreams": []}))),
        )

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_ssot_get_url_valued_query",
            operation_dict=self._ssot_get_operation(path_with_url_cursor),
            credentials=self.credentials,
        )

        self.assertFalse(response.is_error, msg=str(response.result))

    def test_ssot_get_does_not_log_query_string_values(self):
        """Query-string values (pagination cursors, filters) may carry sensitive
        data — logs must only include the path component, never the query."""
        path_with_query = (
            "/services/data/v62.0/ssot/data-streams?cursor=sensitive-cursor-value"
        )
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(return_value=(200, {}, json.dumps({"dataStreams": []}))),
        )

        with self.assertLogs(self._PROXY_CLIENT_LOGGER, level="INFO") as logs:
            response = self.agent.execute_operation(
                connection_type="salesforce-data-cloud",
                operation_name="test_ssot_get_query_not_logged",
                operation_dict=self._ssot_get_operation(path_with_query),
                credentials=self.credentials,
            )

        self.assertFalse(response.is_error, msg=str(response.result))
        log_text = "\n".join(logs.output)
        for record in logs.records:
            self.assertNotIn("sensitive-cursor-value", str(record.__dict__))
        self.assertNotIn("sensitive-cursor-value", log_text)

    def test_ssot_get_non_json_200_raises_runtime_error(self):
        """A 200 response whose body is not JSON surfaces as a RuntimeError that
        names the failure (`non-JSON`) rather than letting the ValueError from
        response.json() propagate raw."""
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(return_value=(200, {}, "<html>not json</html>")),
        )

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_ssot_get_non_json",
            operation_dict=self._ssot_get_operation(self._SSOT_PATH),
            credentials=self.credentials,
        )

        self.assertTrue(response.is_error)
        self.assertIn("non-JSON", str(response.result))

    def test_ssot_get_http_error_redacts_token_in_body(self):
        """A non-200 whose body embeds a token has the token redacted before the
        body is surfaced — `[REDACTED]` appears and the raw token does not."""
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(
                return_value=(
                    403,
                    {},
                    json.dumps(
                        [{"errorCode": "X", "access_token": "secret-should-redact"}]
                    ),
                )
            ),
        )

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_ssot_get_redacts_token",
            operation_dict=self._ssot_get_operation(self._SSOT_PATH),
            credentials=self.credentials,
        )

        self.assertTrue(response.is_error)
        message = str(response.result)
        self.assertIn("[REDACTED]", message)
        self.assertNotIn("secret-should-redact", message)

    # -- ssot_get core-token reuse (YET-2522) ----------------------------------------

    _INVALID_SESSION_BODY = json.dumps(
        [{"errorCode": "INVALID_SESSION_ID", "message": "Session expired or invalid"}]
    )

    # -- Batched offset reads: ssot_get_offset_pages (YET follow-up to YET-2531) -----

    @staticmethod
    def _offset_of(url: str) -> int:
        return int(
            dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))["offset"]
        )

    def _offset_pages_operation(
        self, path: str, limit: int, start_offset: int, page_count: int, **extra: Any
    ) -> dict:
        kwargs: dict = {
            "path": path,
            "limit": limit,
            "start_offset": start_offset,
            "page_count": page_count,
        }
        kwargs.update(extra)
        return {
            "trace_id": "test-trace-id",
            "skip_cache": True,
            "commands": [{"method": "ssot_get_offset_pages", "kwargs": kwargs}],
        }

    def test_ssot_get_offset_pages_returns_per_page_results_in_order(self):
        """The op fetches N consecutive offset pages (offset = start_offset + i*limit)
        and returns them keyed by string index under ``pages`` — the whole result has
        no top-level ``all_results``/``description`` so process_result passes it
        through untouched.

        The per-page GET is stubbed at ``_ssot_get_one`` rather than via ``responses``:
        the op fans out concurrently, and ``responses``' RequestsMock is not
        thread-safe (it races and mis-routes under a real ThreadPoolExecutor), so
        width>1 tests drive the stubbed primitive instead (plan: responses-based
        error-path tests run at width 1)."""

        def stub(_self, path, _request_timeout):
            offset = self._offset_of("x?" + urllib.parse.urlsplit(path).query)
            return {"rows": [f"r{offset}"], "offset": offset}

        with patch.object(
            sfdc_module.SalesforceDataCloudProxyClient,
            "_ssot_get_one",
            autospec=True,
            side_effect=stub,
        ):
            response = self.agent.execute_operation(
                connection_type="salesforce-data-cloud",
                operation_name="test_offset_pages",
                operation_dict=self._offset_pages_operation(
                    self._SSOT_PATH, limit=200, start_offset=0, page_count=3
                ),
                credentials=self.credentials,
            )

        self.assertFalse(response.is_error, msg=str(response.result))
        pages = response.result[ATTRIBUTE_NAME_RESULT]["pages"]
        self.assertEqual(set(pages), {"0", "1", "2"})
        self.assertEqual(pages["0"]["result"]["offset"], 0)
        self.assertEqual(pages["1"]["result"]["offset"], 200)
        self.assertEqual(pages["2"]["result"]["offset"], 400)

    def test_ssot_get_offset_pages_mixed_success_404_and_quota(self):
        """One page failing never fails the op: a 200 is a ``result`` entry, a 404 and
        a 429/REQUEST_LIMIT_EXCEEDED become structured per-page ``error`` entries
        (status/error_code/error_type), and the op still returns is_error False. This
        is the CANONICAL envelope shape the data-collector translation is tested
        against — keep it in sync with the DC fixture ``ssot_offset_pages_envelope``.

        Stubs ``_ssot_get_one_with_retry`` (raising ``SsotGetError`` for the error
        offsets) rather than using ``responses``: the op fans out concurrently and
        ``responses`` is not thread-safe. A barrier makes every worker pass its
        quota stop-flag check BEFORE any page raises, so no page is skipped and all
        three entries are produced deterministically."""
        barrier = threading.Barrier(3, timeout=5)

        def stub(_self, path, _request_timeout):
            offset = self._offset_of("x?" + urllib.parse.urlsplit(path).query)
            barrier.wait()  # all three enter before any raises → no skipped_quota
            if offset == 200:
                raise sfdc_module.SsotGetError(
                    "Salesforce Data Cloud SSOT GET /x failed with code 404",
                    status_code=404,
                    error_code="NOT_FOUND",
                )
            if offset == 400:
                raise sfdc_module.SsotGetError(
                    "Salesforce Data Cloud SSOT GET /x failed with code 429",
                    status_code=429,
                    error_code="REQUEST_LIMIT_EXCEEDED",
                )
            return {"ok": True}

        with patch.object(
            sfdc_module.SalesforceDataCloudProxyClient,
            "_ssot_get_one_with_retry",
            autospec=True,
            side_effect=stub,
        ):
            response = self.agent.execute_operation(
                connection_type="salesforce-data-cloud",
                operation_name="test_offset_pages_mixed",
                operation_dict=self._offset_pages_operation(
                    self._SSOT_PATH, limit=200, start_offset=0, page_count=3
                ),
                credentials=self.credentials,
            )

        self.assertFalse(response.is_error, msg=str(response.result))
        pages = response.result[ATTRIBUTE_NAME_RESULT]["pages"]
        self.assertEqual(pages["0"], {"result": {"ok": True}})
        self.assertEqual(pages["1"]["status_code"], 404)
        self.assertEqual(pages["1"]["error_code"], "NOT_FOUND")
        self.assertEqual(pages["2"]["status_code"], 429)
        self.assertEqual(pages["2"]["error_code"], "REQUEST_LIMIT_EXCEEDED")
        self.assertEqual(pages["2"]["error_type"], "rate_limited")
        # No token leaks into the returned envelope.
        self.assertNotIn(self.client_credentials_token, json.dumps(response.result))

    def test_ssot_get_offset_pages_fetches_pages_concurrently(self):
        """All pages of the batch are in flight at once — pinned with a Barrier the
        stubbed fetch must rendezvous on (a sequential regression breaks the barrier
        and the pages come back as errors, a deterministic failure — not a hang)."""
        page_count = 4
        barrier = threading.Barrier(page_count, timeout=5)

        def stub(_self, path, _request_timeout):
            barrier.wait()
            return {"offset": self._offset_of("x?" + urllib.parse.urlsplit(path).query)}

        with patch.object(
            sfdc_module.SalesforceDataCloudProxyClient,
            "_ssot_get_one",
            autospec=True,
            side_effect=stub,
        ):
            response = self.agent.execute_operation(
                connection_type="salesforce-data-cloud",
                operation_name="test_offset_pages_concurrent",
                operation_dict=self._offset_pages_operation(
                    self._SSOT_PATH, limit=100, start_offset=0, page_count=page_count
                ),
                credentials=self.credentials,
            )

        self.assertFalse(response.is_error, msg=str(response.result))
        pages = response.result[ATTRIBUTE_NAME_RESULT]["pages"]
        self.assertEqual(len(pages), page_count)
        self.assertTrue(all("result" in p for p in pages.values()), msg=str(pages))

    def test_ssot_get_offset_pages_mints_core_token_once_for_the_batch(self):
        """A cold-cache batch mints the core token exactly ONCE (the process-wide
        lock collapses the concurrent first calls, YET-2522) — the property that makes
        agent-side fan-out safe against the token endpoint. The GET is stubbed so the
        assertion is on the real mint path, not on ``responses`` under threads."""
        before = self._mint_returns(str(uuid.uuid4()))

        def stub_request(_self, _session, path, _access_token, _timeout):
            resp = requests.Response()
            resp.status_code = 200
            resp._content = json.dumps({"offset": self._offset_of(path)}).encode()
            return resp

        with patch.object(
            sfdc_module.SalesforceDataCloudProxyClient,
            "_ssot_request",
            autospec=True,
            side_effect=stub_request,
        ):
            response = self.agent.execute_operation(
                connection_type="salesforce-data-cloud",
                operation_name="test_offset_pages_one_mint",
                operation_dict=self._offset_pages_operation(
                    self._SSOT_PATH, limit=200, start_offset=0, page_count=5
                ),
                credentials=self.credentials,
            )

        self.assertFalse(response.is_error, msg=str(response.result))
        self.assertEqual(len(response.result[ATTRIBUTE_NAME_RESULT]["pages"]), 5)
        self.assertEqual(self._mints_since(before), 1)

    def test_ssot_get_offset_pages_retries_a_transient_page_once(self):
        """A per-page transient failure (connection drop) gets one more attempt within
        the same timeout envelope — the resilience the DC's sequential path had via
        _get_tag_page_with_retry, preserved on the batched path so one blip in an
        8-wide wave doesn't become a hole."""
        attempts: dict = collections.defaultdict(int)

        def stub(_self, path, _request_timeout):
            offset = self._offset_of("x?" + urllib.parse.urlsplit(path).query)
            attempts[offset] += 1
            if offset == 200 and attempts[offset] == 1:
                raise requests.exceptions.ConnectionError("transient")
            return {"offset": offset}

        with patch.object(
            sfdc_module.SalesforceDataCloudProxyClient,
            "_ssot_get_one",
            autospec=True,
            side_effect=stub,
        ):
            response = self.agent.execute_operation(
                connection_type="salesforce-data-cloud",
                operation_name="test_offset_pages_retry",
                operation_dict=self._offset_pages_operation(
                    self._SSOT_PATH, limit=200, start_offset=0, page_count=3
                ),
                credentials=self.credentials,
            )

        pages = response.result[ATTRIBUTE_NAME_RESULT]["pages"]
        self.assertTrue(all("result" in p for p in pages.values()), msg=str(pages))
        self.assertEqual(attempts[200], 2)  # failed once, retried, recovered

    def test_ssot_get_offset_pages_does_not_retry_a_quota_page(self):
        """A quota error is NOT retried (retrying hammers an already-exhausted org) —
        it comes back as a per-page error entry the DC maps to its quota breaker."""
        attempts: dict = collections.defaultdict(int)

        def stub(_self, path, _request_timeout):
            offset = self._offset_of("x?" + urllib.parse.urlsplit(path).query)
            attempts[offset] += 1
            if offset == 200:
                raise sfdc_module.SsotGetError(
                    "Salesforce Data Cloud SSOT GET /x failed with code 429",
                    status_code=429,
                    error_code="REQUEST_LIMIT_EXCEEDED",
                )
            return {"offset": offset}

        with patch.object(
            sfdc_module.SalesforceDataCloudProxyClient,
            "_ssot_get_one",
            autospec=True,
            side_effect=stub,
        ):
            response = self.agent.execute_operation(
                connection_type="salesforce-data-cloud",
                operation_name="test_offset_pages_quota",
                operation_dict=self._offset_pages_operation(
                    self._SSOT_PATH, limit=200, start_offset=0, page_count=3
                ),
                credentials=self.credentials,
            )

        pages = response.result[ATTRIBUTE_NAME_RESULT]["pages"]
        self.assertEqual(pages["1"]["status_code"], 429)
        self.assertEqual(pages["1"]["error_type"], "rate_limited")
        self.assertEqual(attempts[200], 1)  # not retried

    def test_ssot_get_offset_pages_clamps_width_and_limit(self):
        """The agent enforces its OWN bounds regardless of what the DC sends:
        page_count clamps to _SSOT_OFFSET_PAGES_MAX and limit to [1, 200]."""
        seen: list = []

        def stub(_self, path, _request_timeout):
            q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(path).query))
            seen.append((int(q["limit"]), int(q["offset"])))
            return {}

        client = self._direct_ssot_client()
        with patch.object(
            sfdc_module.SalesforceDataCloudProxyClient,
            "_ssot_get_one",
            autospec=True,
            side_effect=stub,
        ):
            client.ssot_get_offset_pages(
                self._SSOT_PATH, limit=99999, start_offset=0, page_count=50
            )

        self.assertEqual(
            len(seen), sfdc_module._SSOT_OFFSET_PAGES_MAX
        )  # width clamped to 8
        self.assertTrue(all(limit == 200 for limit, _ in seen))  # limit clamped to 200

    def test_ssot_get_offset_pages_rejects_bad_inputs(self):
        """Invalid inputs fail the whole batch BEFORE any GET: a base path that
        already carries limit/offset, a non-positive page_count."""
        client = self._direct_ssot_client()
        with self.assertRaises(ValueError):
            client.ssot_get_offset_pages(
                self._SSOT_PATH + "?limit=200&offset=0",
                limit=200,
                start_offset=0,
                page_count=2,
            )
        with self.assertRaises(ValueError):
            client.ssot_get_offset_pages(
                self._SSOT_PATH, limit=200, start_offset=0, page_count=0
            )

    def test_ssot_get_offset_pages_preserves_an_existing_query_param(self):
        """A base path with its own query (e.g. ``?dataspace=CSG``) keeps it; the agent
        only appends limit/offset per page."""
        seen: list = []

        def stub(_self, path, _request_timeout):
            seen.append(dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(path).query)))
            return {}

        client = self._direct_ssot_client()
        with patch.object(
            sfdc_module.SalesforceDataCloudProxyClient,
            "_ssot_get_one",
            autospec=True,
            side_effect=stub,
        ):
            client.ssot_get_offset_pages(
                self._SSOT_PATH + "?dataspace=CSG",
                limit=50,
                start_offset=100,
                page_count=2,
            )

        self.assertEqual([q["dataspace"] for q in seen], ["CSG", "CSG"])
        self.assertEqual(sorted(int(q["offset"]) for q in seen), [100, 150])

    def test_ssot_get_offset_pages_page_body_with_description_passes_through(self):
        """A per-page body carrying a top-level ``description`` string round-trips
        unmangled — the op envelope's top level is ``pages`` (not a dbapi shape), and
        process_result only inspects the top level."""

        def by_offset(request):
            return (200, {}, json.dumps({"description": "human text", "value": 1}))

        self.mock_responses.add_callback(
            method=responses.GET, url=self._SSOT_URL, callback=by_offset
        )

        response = self.agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_offset_pages_description",
            operation_dict=self._offset_pages_operation(
                self._SSOT_PATH, limit=200, start_offset=0, page_count=1
            ),
            credentials=self.credentials,
        )

        self.assertFalse(response.is_error, msg=str(response.result))
        pages = response.result[ATTRIBUTE_NAME_RESULT]["pages"]
        self.assertEqual(
            pages["0"]["result"], {"description": "human text", "value": 1}
        )

    def _direct_ssot_client(self, **overrides: str) -> SalesforceDataCloudProxyClient:
        """Build a proxy client directly instead of going through the agent.

        The core-token cache is process-wide, keyed by (domain, client_id,
        client_secret), and shared across client instances (YET-2522). The agent
        path passes `skip_cache=True`, so `ProxyClientFactory` builds a fresh
        client per operation — which is exactly the production boundary the cache
        must survive. Driving the client directly here lets a test observe both
        "two calls, one mint" on one client AND token sharing across separate
        client instances (the real DC path). `overrides` tweak individual
        connect_args (e.g. a different `client_secret`) to exercise cache keying.
        """
        connect_args = {**self.credentials["connect_args"], **overrides}
        return SalesforceDataCloudProxyClient(
            credentials=SalesforceDataCloudCredentials(
                domain=connect_args["domain"],
                client_id=connect_args["client_id"],
                client_secret=connect_args["client_secret"],
                core_token=connect_args.get("core_token"),
                refresh_token=None,
            )
        )

    def _mint_returns(self, *tokens: str) -> int:
        """Make successive client-credentials mints return `tokens` in order.

        Returns the mint count so far, so callers can assert on the delta rather
        than the absolute count (building the client may mint on its own). Once
        `tokens` is exhausted a distinctive sentinel token is returned rather than
        raising `StopIteration` inside the `responses` callback — so a regression
        that mints once too many fails on the caller's mint-count/Bearer
        assertion (a clear diagnosis) instead of an opaque callback error.
        """
        token_iter = iter(tokens)

        def _next_mint(_request: Any):
            token = next(token_iter, "unexpected-extra-mint-token")
            return (
                200,
                {},
                json.dumps(
                    {
                        "access_token": token,
                        "instance_url": "https://test.salesforce.com",
                    }
                ),
            )

        self.client_credentials_token_endpoint.side_effect = _next_mint
        return self.client_credentials_token_endpoint.call_count

    def _mints_since(self, before: int) -> int:
        return self.client_credentials_token_endpoint.call_count - before

    def _ssot_calls(self) -> list:
        return [
            c
            for c in self.mock_responses.calls
            if "/ssot/data-streams" in c.request.url
        ]

    def _cache_key(self, client: SalesforceDataCloudProxyClient) -> str:
        return client._core_token_cache_key

    def test_ssot_get_reuses_the_cached_core_token_across_calls(self):
        """The token endpoint — not the data reads — is what Salesforce throttles
        (`invalid_grant: login rate exceeded`), and YET-2410 made ssot_get per-item,
        so a mint per call is N+1 mints per collection run. Two reads on one client
        must mint ONCE and send the same Bearer credential twice."""
        client = self._direct_ssot_client()
        mints_before = self._mint_returns("first-core-token", "second-core-token")
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(return_value=(200, {}, json.dumps({"dataStreams": []}))),
        )

        client.ssot_get(self._SSOT_PATH)
        client.ssot_get(self._SSOT_PATH)

        self.assertEqual(self._mints_since(mints_before), 1)
        ssot_calls = self._ssot_calls()
        self.assertEqual(len(ssot_calls), 2)
        self.assertEqual(
            {c.request.headers["Authorization"] for c in ssot_calls},
            {"Bearer first-core-token"},
        )

    def test_ssot_get_shares_the_cached_token_across_client_instances(self):
        """The production fix (YET-2522): the data-collector sends `skip_cache=true`,
        so every ssot_get gets a BRAND-NEW proxy client. If the cache were on the
        instance it would never be reused and the N+1 mint storm would persist.
        Two independent clients with identical credentials must therefore share one
        minted token — one mint total across both."""
        first_client = self._direct_ssot_client()
        second_client = self._direct_ssot_client()
        mints_before = self._mint_returns("shared-core-token", "unexpected-second")
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(return_value=(200, {}, json.dumps({"dataStreams": []}))),
        )

        first_client.ssot_get(self._SSOT_PATH)
        second_client.ssot_get(self._SSOT_PATH)

        self.assertEqual(self._mints_since(mints_before), 1)
        self.assertEqual(
            {c.request.headers["Authorization"] for c in self._ssot_calls()},
            {"Bearer shared-core-token"},
        )

    def test_ssot_get_does_not_share_tokens_across_different_credentials(self):
        """The cache key is a hash of (domain, client_id, client_secret), so a
        token minted for one connection is never served to another — no
        cross-tenant credential reuse. Two clients differing only in client_secret
        each mint their own token."""
        client_a = self._direct_ssot_client(client_secret="secret_a")
        client_b = self._direct_ssot_client(client_secret="secret_b")
        self.assertNotEqual(self._cache_key(client_a), self._cache_key(client_b))
        mints_before = self._mint_returns("token-a", "token-b")
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(return_value=(200, {}, json.dumps({"dataStreams": []}))),
        )

        client_a.ssot_get(self._SSOT_PATH)
        client_b.ssot_get(self._SSOT_PATH)

        self.assertEqual(self._mints_since(mints_before), 2)
        self.assertEqual(
            [c.request.headers["Authorization"] for c in self._ssot_calls()],
            ["Bearer token-a", "Bearer token-b"],
        )

    def test_ssot_get_remints_when_the_cached_token_is_older_than_the_max_age(self):
        """The cache is bounded by a soft max age (~60s), mirroring the client-cache
        lifetime the design leaned on, so a token minted under a since-rotated
        secret cannot linger indefinitely. Past that age the next call mints fresh
        even without a 401."""
        client = self._direct_ssot_client()
        mints_before = self._mint_returns("aged-out-token", "fresh-token")
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(return_value=(200, {}, json.dumps({"dataStreams": []}))),
        )

        # First call at t=1000 primes the cache; second at t=1000+max_age+1 is stale.
        # A constant return_value per phase keeps this robust to how many times the
        # implementation happens to read the clock.
        max_age = sfdc_module._CORE_TOKEN_CACHE_MAX_AGE_SECONDS
        with patch.object(sfdc_module.time, "monotonic", return_value=1000.0):
            client.ssot_get(self._SSOT_PATH)
        with patch.object(
            sfdc_module.time, "monotonic", return_value=1000.0 + max_age + 1
        ):
            client.ssot_get(self._SSOT_PATH)

        self.assertEqual(self._mints_since(mints_before), 2)
        self.assertEqual(
            [c.request.headers["Authorization"] for c in self._ssot_calls()],
            ["Bearer aged-out-token", "Bearer fresh-token"],
        )

    def test_ssot_get_prunes_aged_out_entries_for_other_keys_on_mint(self):
        """The cache is pruned on mint so it cannot grow without bound across many
        distinct credential sets: an entry past the reuse max age can never be
        served again (a read would re-mint it), so the next mint for ANY key drops
        it. Without this a long-lived process cycling through many connections would
        accumulate one dead entry per credential set (YET-2522)."""
        client_a = self._direct_ssot_client(client_secret="secret_a")
        client_b = self._direct_ssot_client(client_secret="secret_b")
        self._mint_returns("token-a", "token-b")
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(return_value=(200, {}, json.dumps({"dataStreams": []}))),
        )
        max_age = sfdc_module._CORE_TOKEN_CACHE_MAX_AGE_SECONDS

        # Prime the cache for credential A at t=1000.
        with patch.object(sfdc_module.time, "monotonic", return_value=1000.0):
            client_a.ssot_get(self._SSOT_PATH)
        self.assertIn(self._cache_key(client_a), _CORE_TOKEN_CACHE)

        # A mint for credential B well past A's max age must evict A's dead entry
        # while leaving B's fresh one.
        with patch.object(
            sfdc_module.time, "monotonic", return_value=1000.0 + max_age + 1
        ):
            client_b.ssot_get(self._SSOT_PATH)

        self.assertNotIn(self._cache_key(client_a), _CORE_TOKEN_CACHE)
        self.assertIn(self._cache_key(client_b), _CORE_TOKEN_CACHE)

    def test_ssot_get_remints_once_and_retries_when_the_cached_token_expired(self):
        """Expiry cannot be predicted — Salesforce omits `expires_in` on the
        client-credentials grant and the token dies with the org session (or is
        revoked). So a cached token that comes back 401/INVALID_SESSION_ID is
        discarded, re-minted once, and the idempotent GET retried."""
        client = self._direct_ssot_client()
        mints_before = self._mint_returns("expired-core-token", "renewed-core-token")
        body = {"dataStreams": [{"name": "Web_Engagement", "totalRecords": 42}]}
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(
                side_effect=[
                    (200, {}, json.dumps(body)),  # primes the cache
                    (401, {}, self._INVALID_SESSION_BODY),  # cached token expired
                    (200, {}, json.dumps(body)),  # retry on a fresh token
                ]
            ),
        )

        client.ssot_get(self._SSOT_PATH)
        self.assertEqual(client.ssot_get(self._SSOT_PATH), body)

        self.assertEqual(self._mints_since(mints_before), 2)
        ssot_calls = self._ssot_calls()
        self.assertEqual(len(ssot_calls), 3)
        self.assertEqual(
            ssot_calls[-1].request.headers["Authorization"],
            "Bearer renewed-core-token",
        )

    def test_ssot_get_does_not_remint_when_the_token_was_minted_for_this_call(self):
        """The `from_cache` guard is a key decision: a 401 on a token minted moments
        ago is a real authorization failure (revoked connected app, missing SSOT
        permission), NOT expiry, so it must surface immediately without a re-mint —
        re-minting here would double mint volume against the throttled endpoint.
        A fresh client's very first ssot_get hitting 401 proves the guard holds
        (without it, the retry path would fire and mint a second time)."""
        client = self._direct_ssot_client()
        mints_before = self._mint_returns("only-core-token", "should-not-be-minted")
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(return_value=(401, {}, self._INVALID_SESSION_BODY)),
        )

        with self.assertRaises(RuntimeError) as raised:
            client.ssot_get(self._SSOT_PATH)

        self.assertIn("code 401", str(raised.exception))
        self.assertEqual(self._mints_since(mints_before), 1)
        self.assertEqual(len(self._ssot_calls()), 1)

    def test_ssot_get_retries_at_most_once_on_a_persistent_401(self):
        """The retry is bounded: a 401 that survives the re-mint is a real failure
        (revoked connected app, missing permission) and must surface the existing
        `code 401` RuntimeError rather than minting in a loop — which is the very
        pressure this change exists to relieve. And a token the retry also saw
        rejected must not stay cached: the entry is evicted so the process never
        knowingly holds an INVALID_SESSION token."""
        client = self._direct_ssot_client()
        mints_before = self._mint_returns(
            "first-core-token", "second-core-token", "third-core-token"
        )
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(
                side_effect=[
                    (200, {}, json.dumps({"dataStreams": []})),  # primes the cache
                    (401, {}, self._INVALID_SESSION_BODY),
                    (401, {}, self._INVALID_SESSION_BODY),
                ]
            ),
        )

        client.ssot_get(self._SSOT_PATH)
        with self.assertRaises(RuntimeError) as raised:
            client.ssot_get(self._SSOT_PATH)

        self.assertIn("code 401", str(raised.exception))
        self.assertEqual(self._mints_since(mints_before), 2)
        self.assertEqual(len(self._ssot_calls()), 3)
        # Post-failure state is pinned: the twice-rejected token is not left cached.
        self.assertNotIn(self._cache_key(client), _CORE_TOKEN_CACHE)

    def test_retry_read_timeout_deducts_elapsed_and_floors(self):
        """The re-mint-and-retry must not double the caller's read budget: the DC
        sizes its transport deadman at `30 + timeout` and expects the agent to error
        first (YET-2440). The retry GET's read bound is `timeout` minus the first
        GET's elapsed time — so both reads stay within one `timeout` — floored at
        the minimum so `requests` never gets a non-positive deadline."""
        # Normal case: 10s already spent out of 100 leaves 90 for the retry read.
        self.assertEqual(_retry_read_timeout(100, 10.0), 90)
        # Nothing spent yet: full budget.
        self.assertEqual(_retry_read_timeout(100, 0.0), 100)
        # First GET consumed the whole budget (or more): floor, never <= 0.
        self.assertEqual(_retry_read_timeout(30, 40.0), 1)
        self.assertEqual(_retry_read_timeout(30, 30.0), 1)

    def test_retry_read_timeout_rounds_fractional_elapsed_up(self):
        """Fractional elapsed must be charged in FULL, not truncated: rounding the
        consumed time DOWN (`int(elapsed)`) would over-allocate the retry read by up
        to ~0.999s and let the two reads together exceed one `timeout`, breaking the
        DC's `30 + timeout` deadman guarantee (YET-2440/YET-2522). Rounding up keeps
        the combined read budget within one `timeout`."""
        # 10.1s spent out of 100 must leave AT MOST 89 (not 90 via truncation).
        self.assertEqual(_retry_read_timeout(100, 10.1), 89)
        # Any fraction consumes the whole second it falls in.
        self.assertEqual(_retry_read_timeout(100, 10.9), 89)
        self.assertEqual(_retry_read_timeout(100, 0.001), 99)

    def test_ssot_get_logs_whether_the_core_token_came_from_cache(self):
        """`ssot_core_token_cached` on the SSOT GET log record is how a cache hit
        is confirmed in Datadog — a scalar, per the logging convention."""
        client = self._direct_ssot_client()
        self._mint_returns("first-core-token", "second-core-token")
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(return_value=(200, {}, json.dumps({"dataStreams": []}))),
        )

        with self.assertLogs(self._PROXY_CLIENT_LOGGER, level="INFO") as logs:
            client.ssot_get(self._SSOT_PATH)
            client.ssot_get(self._SSOT_PATH)

        flags = [
            record.ssot_core_token_cached
            for record in logs.records
            if hasattr(record, "ssot_core_token_cached")
        ]
        self.assertEqual(flags, [False, True])

    def test_list_dataspaces_keeps_minting_its_own_core_token(self):
        """Only the ssot_get mints are deduped. `list_dataspaces` runs once per
        collection and stays on its own mint — it neither reads nor writes the
        ssot_get cache. (The per-dataspace `list_tables` path likewise mints per
        dataspace, deliberately, to avoid ambiguous scoping; not exercised here.)"""
        client = self._direct_ssot_client()
        mints_before = self._mint_returns("ssot-core-token", "dataspaces-core-token")
        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(return_value=(200, {}, json.dumps({"dataStreams": []}))),
        )
        self._add_soql_callback(
            Mock(
                return_value=(
                    200,
                    {},
                    json.dumps(
                        {
                            "records": [{"DataSpaceApiName": "default"}],
                            "done": True,
                        }
                    ),
                )
            )
        )

        client.ssot_get(self._SSOT_PATH)
        self.assertEqual(client.list_dataspaces(), ["default"])

        self.assertEqual(self._mints_since(mints_before), 2)
        soql_call = next(
            c
            for c in self.mock_responses.calls
            if "/services/data/v62.0/query" in c.request.url
        )
        self.assertEqual(
            soql_call.request.headers["Authorization"],
            "Bearer dataspaces-core-token",
        )

    def test_is_expired_session_branches(self):
        """`_is_expired_session` gates the re-mint. Cover all three branches:
        a 401 is always expiry; a 200 never is (even if the body mentions the
        string — an auth-object metadata payload must not force a spurious mint);
        and a non-401 error requires a STRUCTURED errorCode, not a substring, so a
        4xx that merely echoes caller-supplied text back does not evict a good
        token."""

        def resp(status: int, body: Any) -> requests.Response:
            r = requests.Response()
            r.status_code = status
            r._content = json.dumps(body).encode() if body is not None else b"<html>"
            return r

        # 401 → expiry regardless of body.
        self.assertTrue(_is_expired_session(resp(401, [{"errorCode": "OTHER"}])))
        # 200 → never expiry, even when the string appears in the payload.
        self.assertFalse(_is_expired_session(resp(200, {"note": "INVALID_SESSION_ID"})))
        # Non-401 error with the structured errorCode → expiry (dict or list form).
        self.assertTrue(
            _is_expired_session(resp(403, [{"errorCode": "INVALID_SESSION_ID"}]))
        )
        self.assertTrue(
            _is_expired_session(resp(500, {"errorCode": "INVALID_SESSION_ID"}))
        )
        # Non-401 error that only REFLECTS the string in a message → not expiry.
        self.assertFalse(
            _is_expired_session(
                resp(
                    400,
                    [{"errorCode": "MALFORMED_QUERY", "message": "INVALID_SESSION_ID"}],
                )
            )
        )
        # Unparseable body → not expiry.
        self.assertFalse(_is_expired_session(resp(500, None)))

    # -- ssot_get read timeout (YET-2440) --------------------------------------------

    def _run_ssot_get_capturing_timeouts(
        self, **operation_kwargs: Any
    ) -> tuple[list[tuple[int, int]], list[int]]:
        """Run one ssot_get op, returning the ``timeout=`` every session GET/POST saw.

        Returns ``(get_timeouts, post_timeouts)`` in call order. The GET list holds
        the Salesforce /ssot read; the POST list holds the client-credentials token
        mint, which must keep its own bound no matter what the caller asks for.

        The GET's ``timeout`` is a ``(connect, read)`` tuple (the connect half is
        pinned to the discovery bound; only the read half is caller-tunable), so
        each captured GET entry is recorded as-is — a 2-tuple — rather than
        unwrapped to just the read value. That keeps the connect pin itself
        assertable and matches what ``requests`` actually receives, at the cost of
        callers writing ``(_DISCOVERY_REQUEST_TIMEOUT_SECONDS, <read>)`` instead of
        a bare int. The mint POST's ``timeout`` stays a bare scalar.
        """
        get_timeouts: list[tuple[int, int]] = []
        post_timeouts: list[int] = []
        real_get = _CapturingSession.get
        real_post = _CapturingSession.post

        def spy_get(session_self, url, **kwargs):
            get_timeouts.append(kwargs.get("timeout"))
            return real_get(session_self, url, **kwargs)

        def spy_post(session_self, url, **kwargs):
            post_timeouts.append(kwargs.get("timeout"))
            return real_post(session_self, url, **kwargs)

        self.mock_responses.add_callback(
            method=responses.GET,
            url=self._SSOT_URL,
            callback=Mock(return_value=(200, {}, json.dumps({"dataStreams": []}))),
        )

        with patch.object(_CapturingSession, "get", spy_get), patch.object(
            _CapturingSession, "post", spy_post
        ):
            response = self.agent.execute_operation(
                connection_type="salesforce-data-cloud",
                operation_name="test_ssot_get_timeout",
                operation_dict=self._ssot_get_operation(
                    self._SSOT_PATH, **operation_kwargs
                ),
                credentials=self.credentials,
            )

        self.assertFalse(response.is_error, msg=str(response.result))
        return get_timeouts, post_timeouts

    def test_ssot_timeout_constants_are_a_data_collector_contract(self):
        """These three constants are a cross-repo contract with the data-collector,
        which budgets `30 + timeout` on its own transport deadman and clamps its
        schedule knob below the agent ceiling. The tests below only assert
        constant-relative equalities (e.g. `[_DISCOVERY_REQUEST_TIMEOUT_SECONDS]`),
        which stay green even if a constant's literal value drifts — so pin the
        literals here too, to make a drift (e.g. re-tuning the default from 30 to
        60) fail loudly instead of passing an unchanged, tautological suite."""
        self.assertEqual(_SSOT_REQUEST_TIMEOUT_DEFAULT_SECONDS, 30)
        self.assertEqual(_DISCOVERY_REQUEST_TIMEOUT_SECONDS, 30)
        self.assertEqual(_SSOT_REQUEST_TIMEOUT_MAX_SECONDS, 300)

    def test_ssot_get_uses_the_default_timeout_when_the_caller_omits_it(self):
        """A data-collector that predates YET-2440 sends no `timeout` kwarg at all
        (as opposed to sending an explicit `None` — see the unusable-timeout test),
        and the GET keeps the historical 30s bound — the parameter is purely
        opt-in."""
        get_timeouts, post_timeouts = self._run_ssot_get_capturing_timeouts()

        self.assertEqual(
            get_timeouts,
            [
                (
                    _DISCOVERY_REQUEST_TIMEOUT_SECONDS,
                    _SSOT_REQUEST_TIMEOUT_DEFAULT_SECONDS,
                )
            ],
        )
        self.assertEqual(post_timeouts, [_DISCOVERY_REQUEST_TIMEOUT_SECONDS])

    def test_ssot_get_honours_a_caller_supplied_timeout_but_not_for_the_mint(self):
        """An explicit `timeout` bounds the READ half of the Salesforce /ssot GET
        only — the connect half stays pinned at the discovery bound. The token mint
        that precedes it keeps its own 30s bound, which is why a caller sizing a
        transport deadman must budget `30 + timeout` rather than `timeout`."""
        get_timeouts, post_timeouts = self._run_ssot_get_capturing_timeouts(timeout=120)

        self.assertEqual(get_timeouts, [(_DISCOVERY_REQUEST_TIMEOUT_SECONDS, 120)])
        self.assertEqual(post_timeouts, [_DISCOVERY_REQUEST_TIMEOUT_SECONDS])

    def test_ssot_get_honours_a_caller_timeout_below_the_default(self):
        """The default (30s) is a fallback for an unusable value, not a floor: a
        caller explicitly asking for a shorter read bound (5s) must get exactly
        that, not be silently raised back up to the default."""
        get_timeouts, _ = self._run_ssot_get_capturing_timeouts(timeout=5)

        self.assertEqual(get_timeouts, [(_DISCOVERY_REQUEST_TIMEOUT_SECONDS, 5)])

    def test_ssot_get_clamps_a_caller_timeout_to_the_agent_ceiling(self):
        """The agent enforces its OWN ceiling rather than trusting the caller to have
        clamped: no data-collector build can pin a worker for longer than this."""
        get_timeouts, _ = self._run_ssot_get_capturing_timeouts(
            timeout=_SSOT_REQUEST_TIMEOUT_MAX_SECONDS * 100
        )

        self.assertEqual(
            get_timeouts,
            [(_DISCOVERY_REQUEST_TIMEOUT_SECONDS, _SSOT_REQUEST_TIMEOUT_MAX_SECONDS)],
        )

    def test_ssot_get_does_not_clamp_a_timeout_exactly_at_the_ceiling(self):
        """The ceiling is inclusive: a value exactly at
        `_SSOT_REQUEST_TIMEOUT_MAX_SECONDS` is a valid caller-supplied bound and must
        pass through unclamped, not be treated as already-over-the-line."""
        get_timeouts, _ = self._run_ssot_get_capturing_timeouts(
            timeout=_SSOT_REQUEST_TIMEOUT_MAX_SECONDS
        )

        self.assertEqual(
            get_timeouts,
            [(_DISCOVERY_REQUEST_TIMEOUT_SECONDS, _SSOT_REQUEST_TIMEOUT_MAX_SECONDS)],
        )

    def test_ssot_get_falls_back_to_the_default_for_an_unusable_timeout(self):
        """`timeout` arrives inside the DC-supplied payload, so it is untrusted in the
        same way `path` is. Anything that is not a positive int degrades to the
        default instead of reaching `requests`. `True` is covered explicitly: bool
        subclasses int, so a JSON `true` would otherwise mean a 1-second timeout.
        `None` is covered explicitly too: it's the most likely non-int shape a
        data-collector would put on the wire for `{"timeout": null}` built
        unconditionally from an optional config — a different dispatch path than
        omitting the kwarg entirely (see the "omits it" test above), even though
        both collapse to the same `_resolve_ssot_timeout(None)` call."""
        for bad in (None, 0, -5, "120", 1.5, True, [120], {"seconds": 120}):
            with self.subTest(timeout=bad):
                get_timeouts, _ = self._run_ssot_get_capturing_timeouts(timeout=bad)
                self.assertEqual(
                    get_timeouts,
                    [
                        (
                            _DISCOVERY_REQUEST_TIMEOUT_SECONDS,
                            _SSOT_REQUEST_TIMEOUT_DEFAULT_SECONDS,
                        )
                    ],
                    f"{bad!r} should degrade to the default timeout",
                )

    def test_ssot_get_logs_the_resolved_timeout_not_the_requested_one(self):
        """The `ssot_timeout` log field is the observability contract for the whole
        rollout — it's how prod verifies a caller-supplied timeout actually took
        effect. Use the clamp case, where resolved != requested, so asserting the
        logged value actually proves it's the resolved one and not an echo of the
        caller's raw input."""
        requested = _SSOT_REQUEST_TIMEOUT_MAX_SECONDS * 100
        with self.assertLogs(self._PROXY_CLIENT_LOGGER, level="INFO") as logs:
            self._run_ssot_get_capturing_timeouts(timeout=requested)

        ssot_get_records = [
            record for record in logs.records if "ssot_timeout" in record.__dict__
        ]
        self.assertTrue(ssot_get_records, "expected a log record carrying ssot_timeout")
        for record in ssot_get_records:
            resolved = record.__dict__["ssot_timeout"]
            self.assertEqual(resolved, _SSOT_REQUEST_TIMEOUT_MAX_SECONDS)
            self.assertNotEqual(resolved, requested)

    def test_redact_body_masks_sensitive_keys(self):
        """`_redact_body` masks double-quoted access_token / id_token values (the
        raw response.text form) while leaving non-sensitive fields intact."""
        from apollo.integrations.db.salesforce_data_cloud_proxy_client import (
            _redact_body,
        )

        body = json.dumps(
            {
                "access_token": "secret-access",
                "id_token": "secret-id",
                "instance_url": "https://test.salesforce.com",
            }
        )
        redacted = _redact_body(body)
        self.assertIsNotNone(redacted)
        self.assertNotIn("secret-access", redacted)
        self.assertNotIn("secret-id", redacted)
        self.assertIn("[REDACTED]", redacted)
        # Non-sensitive fields are preserved.
        self.assertIn("instance_url", redacted)
        self.assertIn("https://test.salesforce.com", redacted)


class SalesforceDataCloudRetryTests(TestCase):
    """Cover the transient-network-error retry that wraps cursor and list_tables.

    The motivating failure: hourly metric monitors fail intermittently with
    ``AgentClientError. ('Connection aborted.', RemoteDisconnected('Remote end
    closed connection without response'))``. A pooled keep-alive connection that
    Salesforce's edge LB has silently closed gets reused → urllib3 raises
    ``ProtocolError(RemoteDisconnected(...))`` before any HTTP response. Retrying
    the call uses a fresh pooled connection and typically succeeds.
    """

    def test_cursor_execute_retries_on_remote_disconnected(self):
        """A single ``RemoteDisconnected`` during ``cursor.execute`` is recovered
        without surfacing the error to the caller."""
        # Build a cursor against a fake connection — we patch QuerySubmitter
        # directly so the cursor's HTTP path is exercised in isolation.
        connection = Mock()
        connection.closed = False
        cursor = _RetryingSalesforceDataCloudCursor(connection)

        json_results = {
            "data": [["a"]],
            "metadata": {"col": {"type": "VARCHAR", "placeInOrder": 0, "typeCode": 12}},
            "done": True,
            "rowCount": 1,
        }
        attempts = {"n": 0}

        def flaky_execute(_connection, _query):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise urllib3.exceptions.ProtocolError(
                    "Connection aborted.",
                    http.client.RemoteDisconnected(
                        "Remote end closed connection without response"
                    ),
                )
            return json_results

        with (
            patch(
                "salesforcecdpconnector.cursor.QuerySubmitter.execute",
                side_effect=flaky_execute,
            ),
            patch("retry.api.time.sleep"),
        ):
            cursor.execute("select 1")

        self.assertEqual(attempts["n"], 2)
        self.assertTrue(cursor.has_result)

    def test_cursor_execute_propagates_after_retry_budget_exhausted(self):
        connection = Mock()
        connection.closed = False
        cursor = _RetryingSalesforceDataCloudCursor(connection)

        def always_fail(_connection, _query):
            raise urllib3.exceptions.ProtocolError(
                "Connection aborted.",
                http.client.RemoteDisconnected("closed"),
            )

        with (
            patch(
                "salesforcecdpconnector.cursor.QuerySubmitter.execute",
                side_effect=always_fail,
            ),
            patch("retry.api.time.sleep"),
        ):
            with self.assertRaises(urllib3.exceptions.ProtocolError):
                cursor.execute("select 1")

    def test_cursor_fetchall_retries_on_transient_error(self):
        """``fetchall`` (which paginates via ``QuerySubmitter.get_next_batch``)
        also retries on transient errors."""
        connection = Mock()
        connection.closed = False
        cursor = _RetryingSalesforceDataCloudCursor(connection)
        # Prime the cursor as if execute() succeeded with one batch and more to fetch.
        cursor.has_result = True
        cursor.has_next = True
        cursor.next_batch_id = "batch-1"
        cursor.data = [["row0"]]
        cursor.description = []

        next_batch = {"data": [["row1"]], "done": True, "metadata": {}}
        attempts = {"n": 0}

        def flaky_next_batch(_connection, _batch_id):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise ConnectionResetError("connection reset by peer")
            return next_batch

        with (
            patch(
                "salesforcecdpconnector.cursor.QuerySubmitter.get_next_batch",
                side_effect=flaky_next_batch,
            ),
            patch("retry.api.time.sleep"),
        ):
            result = cursor.fetchall()

        self.assertEqual(attempts["n"], 2)
        self.assertEqual(result, [["row0"], ["row1"]])

    def test_list_tables_retries_on_transient_error(self):
        """``SalesforceDataCloudProxyClient.list_tables`` (unscoped path) recovers
        from a single transient ``RemoteDisconnected`` raised by the underlying
        connection. Confirms the ``list_tables`` retry wrapper at the proxy
        layer (the cursor retry doesn't cover this path)."""
        from apollo.integrations.db.salesforce_data_cloud_proxy_client import (
            SalesforceDataCloudProxyClient,
            SalesforceDataCloudCredentials,
        )

        credentials = SalesforceDataCloudCredentials(
            domain="test.salesforce.com",
            client_id="cid",
            client_secret="csec",
            core_token="t",
            refresh_token=None,
        )
        # Bypass the real connection construction; we only need the
        # SalesforceDataCloudProxyClient instance to exercise its list_tables.
        with patch(
            "apollo.integrations.db.salesforce_data_cloud_proxy_client."
            "SalesforceDataCloudConnection"
        ) as conn_class:
            attempts = {"n": 0}
            fake_table = Mock()
            fake_table.name = "Account"
            fake_table.display_name = "Account"
            fake_table.category = "OBJECT"
            fake_table.fields = []

            def flaky_list_tables():
                attempts["n"] += 1
                if attempts["n"] == 1:
                    raise urllib3.exceptions.ProtocolError(
                        "Connection aborted.",
                        http.client.RemoteDisconnected(
                            "Remote end closed connection without response"
                        ),
                    )
                return [fake_table]

            conn_instance = Mock()
            conn_instance.list_tables.side_effect = flaky_list_tables
            conn_class.return_value = conn_instance

            client = SalesforceDataCloudProxyClient(credentials=credentials)

            with patch("retry.api.time.sleep"):
                result = client.list_tables()

        self.assertEqual(attempts["n"], 2)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "Account")


class SalesforceDataCloudProcessResultTests(TestCase):
    """`process_result` must transform Data Cloud cursor results and leave REST bodies alone.

    This client serves both: query ops via `SalesforceCDPCursor`, and `ssot_get`, which returns
    a Salesforce resource body verbatim. The override discriminates by shape, which is exact
    here because Data Cloud descriptions are plain 7-tuples built by the connector
    (`query_result_parser._get_description_item`) — see the base-class test for why the same
    check must NOT live one level up (YET-2410).
    """

    def setUp(self) -> None:
        from apollo.integrations.db.salesforce_data_cloud_proxy_client import (
            SalesforceDataCloudProxyClient,
        )

        # No connection is needed: process_result is pure. Bypass __init__ so the test does
        # not stand up a SalesforceCDPConnection.
        self.client = SalesforceDataCloudProxyClient.__new__(
            SalesforceDataCloudProxyClient
        )
        self.client._connection = None

    def test_cursor_result_is_transformed(self):
        """The real Data Cloud cursor shape — a list of 7-tuples, exactly what
        `_get_description_item` emits — still gets serialized to lists."""
        processed = self.client.process_result(
            {
                "description": [("Id__c", "VARCHAR", None, None, None, None, None)],
                "all_results": [["a"], ["b"]],
            }
        )
        self.assertEqual(
            processed["description"],
            [["Id__c", "VARCHAR", None, None, None, None, None]],
        )
        self.assertEqual(processed["all_results"], [["a"], ["b"]])

    def test_row_less_cursor_result_keeps_empty_list_shape(self):
        """`None` on a cursor result is still coerced to `[]` by the base class."""
        processed = self.client.process_result(
            {"description": None, "all_results": None}
        )
        self.assertEqual(processed["description"], [])
        self.assertEqual(processed["all_results"], [])

    def test_rest_body_with_description_string_passes_through(self):
        """The failure this override exists for: a Retriever detail body whose top-level
        `description` is prose. The base class iterated the string and indexed each
        character, raising `IndexError: string index out of range`."""
        body = {
            "name": "WebRetrievalAction",
            "description": "Web search retriever retrieves search results from the internet.",
            "activeConfiguration": {
                "name": "WebRetrievalActionVersion",
                "isActive": True,
            },
        }
        self.assertEqual(self.client.process_result(json.loads(json.dumps(body))), body)

    def test_rest_body_with_string_rows_is_not_exploded_into_characters(self):
        """`all_results` gets the same element-level check as `description`. A REST body
        carrying strings under that key used to be silently rewritten to lists of
        characters — no exception, just corrupted data."""
        body = {"all_results": ["alpha", "beta"]}
        self.assertEqual(self.client.process_result(json.loads(json.dumps(body))), body)

    def test_rest_body_with_object_rows_keeps_its_objects(self):
        """A list of JSON objects under `all_results` used to be reduced to its keys."""
        body = {"all_results": [{"id": 1, "label": "x"}]}
        self.assertEqual(self.client.process_result(json.loads(json.dumps(body))), body)

    def test_rest_body_with_null_description_keeps_null(self):
        """A REST body that genuinely carried `null` is not rewritten to `[]` — that
        coercion is a cursor-result contract, not a passthrough one. Distinguished from a
        row-less cursor by the absence of any other cursor key."""
        body = {"name": "Retriever_With_No_Description", "description": None}
        self.assertEqual(self.client.process_result(json.loads(json.dumps(body))), body)

    def test_rest_body_with_short_sequence_description_passes_through(self):
        """A `description` that is a list of non-column sequences is not a cursor result."""
        body = {"description": [["too", "short"]], "name": "X"}
        self.assertEqual(self.client.process_result(json.loads(json.dumps(body))), body)

    def test_body_without_cursor_keys_is_untouched(self):
        body = {"name": "SalesforceHelpContentRetriever", "isGlobal": True}
        self.assertEqual(self.client.process_result(json.loads(json.dumps(body))), body)

    def test_non_dict_results_are_delegated_unchanged(self):
        """`ssot_get` can return a top-level JSON list for some core REST endpoints."""
        self.assertEqual(self.client.process_result([{"a": 1}]), [{"a": 1}])
