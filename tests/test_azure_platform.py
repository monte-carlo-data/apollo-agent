import json
import os
from datetime import datetime, timezone, timedelta
from unittest import TestCase
from unittest.mock import patch, Mock

from box import Box

from apollo.agent.agent import Agent
from apollo.agent.constants import ATTRIBUTE_NAME_ERROR, ATTRIBUTE_NAME_RESULT
from apollo.agent.env_vars import IS_REMOTE_UPGRADABLE_ENV_VAR
from apollo.agent.logging_utils import LoggingUtils
from apollo.interfaces.azure.azure_platform import AzurePlatformProvider
from apollo.interfaces.azure.azure_updater import AzureUpdater


class TestAzurePlatform(TestCase):
    @patch.object(AzureUpdater, "_get_resource_management_client")
    @patch.dict(
        os.environ,
        {
            "WEBSITE_RESOURCE_GROUP": "rg",
            "WEBSITE_SITE_NAME": "test_function",
        },
    )
    def test_get_infra_details(self, mock_arm_client):
        agent = Agent(LoggingUtils())
        agent.platform_provider = AzurePlatformProvider()

        mock_client = Mock()
        mock_arm_client.return_value = mock_client
        mock_resource = Mock()
        mock_client.resources.get.return_value = mock_resource
        resource = {"id": "123"}
        mock_resource.as_dict.return_value = resource

        response = agent.get_infra_details("123")

        mock_client.resources.get.assert_called_with(
            resource_group_name="rg",
            resource_provider_namespace="Microsoft.Web",
            parent_resource_path="sites",
            resource_type="",
            resource_name="test_function",
            api_version="2022-03-01",
        )

        self.assertIsNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        result = response.result.get(ATTRIBUTE_NAME_RESULT)
        self.assertEqual(resource, result.get("resource"))

    @patch.object(AzureUpdater, "_get_resource_management_client")
    @patch.dict(
        os.environ,
        {
            "WEBSITE_RESOURCE_GROUP": "rg",
            "WEBSITE_SITE_NAME": "test_function",
        },
    )
    @patch("apollo.interfaces.azure.azure_platform.LogsQueryClient")
    def test_logs_query(self, mock_logs_client, mock_arm_client):
        platform_provider = AzurePlatformProvider()
        start_time = datetime.now(timezone.utc) - timedelta(minutes=20)
        end_time = datetime.now(timezone.utc)

        expected_events = [
            {
                "timestamp": 1,
                "message": "abc",
                "customDimensions": {
                    "mcd_trace_id": "123",
                },
            },
            {
                "timestamp": 2,
                "message": "def",
                "customDimensions": {
                    "mcd_trace_id": "321",
                },
            },
        ]

        mock_client = Mock()
        mock_arm_client.return_value = mock_client
        mock_resource = Mock()
        mock_client.resources.get.return_value = mock_resource
        resource = {
            "id": "123",
            "tags": {
                "hidden-link: /app-insights-resource-id": "app-insights-resource-id",
            },
        }
        mock_resource.as_dict.return_value = resource

        mock_logs_client_instance = Mock()
        mock_logs_client.return_value = mock_logs_client_instance
        mock_logs_client_instance.query_resource.return_value = Box(
            {
                "tables": [
                    {
                        "rows": [
                            {
                                "timestamp": 1,
                                "message": "abc",
                                "customDimensions": '{"mcd_trace_id": "123"}',
                            },
                            {
                                "timestamp": 2,
                                "message": "def",
                                "customDimensions": '{"mcd_trace_id": "321"}',
                            },
                        ],
                        "columns": [
                            "timestamp",
                            "message",
                            "customDimensions",
                        ],
                    }
                ]
            }
        )

        result = platform_provider.get_logs(
            query=None,
            start_time_str=start_time.isoformat(),
            end_time_str=end_time.isoformat(),
            limit=10,
        )
        self.assertEqual(expected_events, result)

        expected_query = (
            f"traces | project timestamp, message, customDimensions"
            f"| order by timestamp desc"
            f"| take 10 "
            f"| order by timestamp asc"
        )
        mock_logs_client_instance.query_resource.assert_called_with(
            resource_id="app-insights-resource-id",
            query=expected_query,
            timespan=(start_time, end_time),
        )

        result = platform_provider.get_logs(
            query='where customDimensions.mcd_trace_id == "1234"',
            start_time_str=start_time.isoformat(),
            end_time_str=end_time.isoformat(),
            limit=10,
        )
        self.assertEqual(expected_events, result)

        expected_query = (
            f'traces | where customDimensions.mcd_trace_id == "1234" | project timestamp, message, customDimensions'
            f"| order by timestamp desc"
            f"| take 10 "
            f"| order by timestamp asc"
        )
        mock_logs_client_instance.query_resource.assert_called_with(
            resource_id="app-insights-resource-id",
            query=expected_query,
            timespan=(start_time, end_time),
        )

        result = platform_provider.get_logs(
            query="traces | order by timestamp asc",
            start_time_str=start_time.isoformat(),
            end_time_str=end_time.isoformat(),
            limit=10,
        )
        self.assertEqual(expected_events, result)

        expected_query = "traces | order by timestamp asc"
        mock_logs_client_instance.query_resource.assert_called_with(
            resource_id="app-insights-resource-id",
            query=expected_query,
            timespan=(start_time, end_time),
        )

    @patch.object(AzureUpdater, "_get_resource_management_client")
    @patch.dict(
        os.environ,
        {
            "WEBSITE_RESOURCE_GROUP": "rg",
            "WEBSITE_SITE_NAME": "test_function",
        },
    )
    @patch("apollo.interfaces.azure.azure_platform.LogsQueryClient")
    def test_logs_parsing(self, mock_logs_client, mock_arm_client):
        platform_provider = AzurePlatformProvider()
        start_time = datetime.now(timezone.utc) - timedelta(minutes=20)
        end_time = datetime.now(timezone.utc)

        commands = [
            {
                "target": "_cursor",
                "method": "cursor",
            },
            {
                "target": "_cursor",
                "method": "execute",
                "args": [
                    "select * from table",
                ],
            },
        ]
        expected_events = [
            {
                "timestamp": 1,
                "message": "abc",
                "customDimensions": {
                    "mcd_trace_id": "123",
                    "commands": commands,
                },
            },
            {
                "timestamp": 2,
                "message": "def",
                "customDimensions": {
                    "mcd_trace_id": "321",
                    "commands": "invalid json",
                },
            },
        ]

        mock_client = Mock()
        mock_arm_client.return_value = mock_client
        mock_resource = Mock()
        mock_client.resources.get.return_value = mock_resource
        resource = {
            "id": "123",
            "tags": {
                "hidden-link: /app-insights-resource-id": "app-insights-resource-id",
            },
        }
        mock_resource.as_dict.return_value = resource

        mock_logs_client_instance = Mock()
        mock_logs_client.return_value = mock_logs_client_instance
        mock_logs_client_instance.query_resource.return_value = Box(
            {
                "tables": [
                    {
                        "rows": [
                            {
                                "timestamp": 1,
                                "message": "abc",
                                "customDimensions": json.dumps(
                                    {
                                        "mcd_trace_id": "123",
                                        "commands": json.dumps(commands),
                                    }
                                ),
                            },
                            {
                                "timestamp": 2,
                                "message": "def",
                                "customDimensions": '{"mcd_trace_id": "321", "commands": "invalid json"}',
                            },
                        ],
                        "columns": [
                            "timestamp",
                            "message",
                            "customDimensions",
                        ],
                    }
                ]
            }
        )

        result = platform_provider.get_logs(
            query=None,
            start_time_str=start_time.isoformat(),
            end_time_str=end_time.isoformat(),
            limit=10,
        )
        self.assertEqual(expected_events, result)

    @patch.object(AzureUpdater, "_get_resource_management_client")
    @patch.dict(
        os.environ,
        {
            "WEBSITE_RESOURCE_GROUP": "rg",
            "WEBSITE_SITE_NAME": "test_function",
            IS_REMOTE_UPGRADABLE_ENV_VAR: "true",
        },
    )
    def test_update(self, mock_arm_client):
        agent = Agent(LoggingUtils())
        agent.platform_provider = AzurePlatformProvider()

        prev_image = "docker.io/montecarlodata/pre-release-agent:0.2.4rc674-azure"
        new_image = "docker.io/montecarlodata/pre-release-agent:0.2.4rc675-azure"

        mock_client = Mock()
        mock_arm_client.return_value = mock_client
        mock_resource = Mock()
        mock_client.resources.get.return_value = mock_resource
        resource = {
            "id": "123",
            "properties": {
                "siteConfig": {
                    "linuxFxVersion": f"DOCKER|{prev_image}",
                },
            },
        }
        mock_resource.as_dict.return_value = resource

        current_image = agent.platform_provider.updater.get_current_image()
        self.assertEqual(f"DOCKER|{prev_image}", current_image)

        update_result = agent.update("1234", image=new_image, timeout_seconds=None)

        update_properties = {
            "properties": {"siteConfig": {"linuxFxVersion": f"DOCKER|{new_image}"}}
        }
        mock_client.resources.begin_update.assert_called_with(
            resource_group_name="rg",
            resource_provider_namespace="Microsoft.Web",
            parent_resource_path="sites",
            resource_type="",
            resource_name="test_function",
            api_version="2022-03-01",
            parameters=json.dumps(update_properties).encode("utf-8"),
        )
        expected_result = {"message": f"Update in progress, image: {new_image}"}
        self.assertEqual(
            expected_result, update_result.result.get(ATTRIBUTE_NAME_RESULT)
        )
