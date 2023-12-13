import os
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import patch, Mock, ANY, call

from apollo.agent.agent import Agent
from apollo.agent.constants import ATTRIBUTE_NAME_ERROR, ATTRIBUTE_NAME_RESULT
from apollo.agent.env_vars import AWS_LAMBDA_FUNCTION_NAME_ENV_VAR
from apollo.agent.logging_utils import LoggingUtils
from apollo.interfaces.lambda_function.platform import AwsPlatformProvider


class TestAwsPlatform(TestCase):
    @patch("boto3.client")
    @patch.dict(
        os.environ,
        {
            "MCD_STACK_ID": "arn:stack_id",
            "MCD_AGENT_WRAPPER_TYPE": "CLOUDFORMATION",
        },
    )
    def test_get_infra_details_cloudformation(self, mock_boto_client):
        agent = Agent(LoggingUtils())
        agent.platform_provider = AwsPlatformProvider()

        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_template.return_value = {"TemplateBody": "template_body"}
        mock_client.describe_stacks.return_value = {
            "Stacks": [
                {
                    "Parameters": [
                        {
                            "ParameterKey": "ImageUri",
                            "ParameterValue": "image_uri",
                        }
                    ],
                    "StackStatus": "UPDATE_COMPLETE",
                }
            ]
        }

        response = agent.get_infra_details("123")
        result = response.result.get(ATTRIBUTE_NAME_RESULT)
        self.assertIsNone(result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual("template_body", result.get("template"))
        self.assertEqual(
            [
                {
                    "ParameterKey": "ImageUri",
                    "ParameterValue": "image_uri",
                },
            ],
            result.get("parameters"),
        )

    @patch("boto3.client")
    @patch.dict(
        os.environ,
        {
            AWS_LAMBDA_FUNCTION_NAME_ENV_VAR: "test_function",
        },
    )
    def test_get_infra_details_no_cloudformation(self, mock_boto_client):
        agent = Agent(LoggingUtils())
        agent.platform_provider = AwsPlatformProvider()

        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_function.return_value = {
            "Configuration": {
                "MemorySize": 123,
            },
            "Concurrency": {
                "ReservedConcurrentExecutions": 12,
            },
        }

        response = agent.get_infra_details("123")
        result = response.result.get(ATTRIBUTE_NAME_RESULT)
        self.assertIsNone(result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual(
            {
                "MemorySize": 123,
                "ConcurrentExecutions": 12,
            },
            result.get("parameters"),
        )

    @patch("boto3.client")
    @patch.dict(
        os.environ,
        {"MCD_LOG_GROUP_ID": "arn:log_group"},
    )
    def test_filter_logs(self, mock_boto_client):
        platform_provider = AwsPlatformProvider()

        expected_events = [
            {"ts": 1, "message": "abc"},
            {"ts": 2, "message": "def"},
            {"ts": 3, "message": "xyz"},
        ]
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.filter_log_events.return_value = {"events": expected_events}

        result = platform_provider.filter_log_events(
            pattern=None,
            start_time_str=None,
            end_time_str=None,
            limit=10,
        )
        mock_client.filter_log_events.assert_called_once_with(
            logGroupIdentifier="arn:log_group",
            limit=10,
            startTime=ANY,
        )
        self.assertEqual(expected_events, result.get("events"))

    @patch("boto3.client")
    @patch.dict(
        os.environ,
        {"MCD_LOG_GROUP_ID": "arn:log_group"},
    )
    def test_filter_logs_paging(self, mock_boto_client):
        platform_provider = AwsPlatformProvider()

        expected_events_1 = [
            {"ts": 1, "message": "abc"},
            {"ts": 2, "message": "def"},
            {"ts": 3, "message": "xyz"},
        ]
        expected_events_2 = [
            {"ts": 4, "message": "opq"},
            {"ts": 5, "message": "rst"},
        ]
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.filter_log_events.side_effect = [
            {"events": expected_events_1, "nextToken": "123"},
            {"events": expected_events_2},
        ]

        start_time = datetime.fromisoformat("2023-12-01T00:00:00Z")
        end_time = datetime.fromisoformat("2023-12-01T10:23:35Z")
        result = platform_provider.filter_log_events(
            pattern="%text%",
            start_time_str=start_time.isoformat(),
            end_time_str=end_time.isoformat(),
            limit=50,
        )

        epoch = datetime.utcfromtimestamp(0).astimezone(timezone.utc)
        start_time_millis = int((start_time - epoch).total_seconds() * 1000)
        end_time_millis = int((end_time - epoch).total_seconds() * 1000)

        mock_client.filter_log_events.assert_has_calls(
            [
                call(
                    logGroupIdentifier="arn:log_group",
                    limit=50,
                    filterPattern="%text%",
                    startTime=start_time_millis,
                    endTime=end_time_millis,
                ),
                call(
                    logGroupIdentifier="arn:log_group",
                    limit=50,
                    filterPattern="%text%",
                    startTime=start_time_millis,
                    endTime=end_time_millis,
                    nextToken="123",
                ),
            ]
        )
        self.assertEqual([*expected_events_1, *expected_events_2], result.get("events"))

    @patch("boto3.client")
    @patch.dict(
        os.environ,
        {"MCD_LOG_GROUP_ID": "arn:log_group"},
    )
    def test_logs_start_query(self, mock_boto_client):
        platform_provider = AwsPlatformProvider()

        expected_query_id = "123"
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.start_query.return_value = {
            "queryId": expected_query_id,
        }
        result = platform_provider.start_logs_query(
            query="fields @timestamp, @message",
            start_time_str=None,
            end_time_str=None,
            limit=10,
        )

        mock_client.start_query.assert_called_once_with(
            logGroupIdentifiers=["arn:log_group"],
            queryString="fields @timestamp, @message",
            startTime=ANY,
            endTime=ANY,
            limit=10,
        )
        self.assertEqual(expected_query_id, result.get("query_id"))

    @patch("boto3.client")
    @patch.dict(
        os.environ,
        {"MCD_LOG_GROUP_ID": "arn:log_group"},
    )
    def test_logs_query_results(self, mock_boto_client):
        platform_provider = AwsPlatformProvider()

        query_id = "123"
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_query_results.return_value = {
            "status": "Running",
        }
        result = platform_provider.get_logs_query_results(
            query_id=query_id,
        )

        mock_client.get_query_results.assert_called_once_with(queryId=query_id)
        self.assertEqual("Running", result.get("status"))
        self.assertEqual([], result.get("events"))

        mock_client.reset_mock()
        mock_client.get_query_results.return_value = {
            "status": "Complete",
            "results": [
                [
                    {
                        "field": "@timestamp",
                        "value": "2023-12-01T00:01:02Z",
                    },
                    {
                        "field": "@message",
                        "value": "message_1",
                    },
                ],
                [
                    {
                        "field": "@timestamp",
                        "value": "2023-12-01T00:01:03Z",
                    },
                    {
                        "field": "@message",
                        "value": "message_2",
                    },
                ],
            ],
        }
        result = platform_provider.get_logs_query_results(
            query_id=query_id,
        )

        mock_client.get_query_results.assert_called_once_with(queryId=query_id)
        self.assertEqual("Complete", result.get("status"))
        self.assertEqual(
            [
                {"@message": "message_1", "@timestamp": "2023-12-01T00:01:02Z"},
                {"@message": "message_2", "@timestamp": "2023-12-01T00:01:03Z"},
            ],
            result.get("events"),
        )
