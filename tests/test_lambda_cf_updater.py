import os
from datetime import datetime, timezone, timedelta
from unittest import TestCase
from unittest.mock import patch, Mock, ANY, call

from apollo.agent.env_vars import CLOUDFORMATION_STACK_ID_ENV_VAR
from apollo.interfaces.lambda_function.lambda_cf_updater import LambdaCFUpdater


class TestLambdaCFUpdater(TestCase):
    @patch.dict(
        os.environ,
        {
            CLOUDFORMATION_STACK_ID_ENV_VAR: "cf_stack_id",
        },
    )
    @patch("boto3.client")
    def test_get_image_uri(self, mock_boto_client):
        expected_image_uri = (
            "123_account.dkr.ecr.us-east-1.amazonaws.com/repo-name:tag-name"
        )
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.describe_stacks.return_value = {
            "Stacks": [
                {
                    "Parameters": [
                        {
                            "ParameterKey": "ImageUri",
                            "ParameterValue": expected_image_uri,
                        }
                    ]
                }
            ]
        }
        updater = LambdaCFUpdater()
        uri = updater.get_current_image(None)
        mock_client.describe_stacks.assert_called_once_with(StackName="cf_stack_id")
        self.assertEqual(expected_image_uri, uri)

    @patch.dict(
        os.environ,
        {
            CLOUDFORMATION_STACK_ID_ENV_VAR: "cf_stack_id",
        },
    )
    @patch("boto3.client")
    def test_update(self, mock_boto_client):
        prev_image_uri = (
            "123_account.dkr.ecr.us-east-1.amazonaws.com/repo-name:tag-name"
        )
        new_image_uri = (
            "123_account.dkr.ecr.us-east-1.amazonaws.com/repo-name:new-tag-name"
        )
        updated_new_image_uri = (
            "123_account.dkr.ecr.*.amazonaws.com/repo-name:new-tag-name"
        )

        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.describe_stacks.return_value = {
            "Stacks": [
                {
                    "Parameters": [
                        {
                            "ParameterKey": "ImageUri",
                            "ParameterValue": prev_image_uri,
                        }
                    ],
                    "StackStatus": "UPDATE_COMPLETE",
                }
            ]
        }
        mock_client.describe_stack_events.return_value = {"StackEvents": []}
        updater = LambdaCFUpdater()
        response = updater.update(
            platform_info=None, image=new_image_uri, timeout_seconds=10
        )

        mock_client.describe_stack_events.assert_called_once_with(
            StackName="cf_stack_id"
        )
        mock_client.describe_stacks.assert_called_with(StackName="cf_stack_id")
        self.assertEqual("UPDATE_COMPLETE", response["status"])
        self.assertEqual(updated_new_image_uri, response["image_uri"])
        self.assertEqual([], response["events"])

    @patch.dict(
        os.environ,
        {
            CLOUDFORMATION_STACK_ID_ENV_VAR: "cf_stack_id",
        },
    )
    @patch("boto3.client")
    def test_update_events(self, mock_boto_client):
        prev_image_uri = (
            "123_account.dkr.ecr.us-east-1.amazonaws.com/repo-name:tag-name"
        )
        new_image_uri = (
            "123_account.dkr.ecr.us-east-1.amazonaws.com/repo-name:new-tag-name"
        )

        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.describe_stacks.return_value = {
            "Stacks": [
                {
                    "Parameters": [
                        {
                            "ParameterKey": "ImageUri",
                            "ParameterValue": prev_image_uri,
                        }
                    ],
                    "StackStatus": "UPDATE_COMPLETE",
                }
            ]
        }
        start_time = datetime.now(timezone.utc)
        mock_client.describe_stack_events.return_value = {
            "StackEvents": [
                {
                    "Timestamp": start_time + timedelta(seconds=11),
                    "LogicalResourceId": "1",
                    "ResourceStatus": "UPDATE_COMPLETE",
                    "ResourceType": "Bucket",
                    "ResourceStatusReason": "OK",
                },
                {
                    "Timestamp": start_time + timedelta(seconds=10),
                    "LogicalResourceId": "2",
                    "ResourceStatus": "UPDATE_COMPLETE",
                    "ResourceType": "Lambda",
                    "ResourceStatusReason": "OK",
                },
                {
                    "Timestamp": start_time - timedelta(seconds=1),
                    "LogicalResourceId": "3",
                },
            ]
        }
        updater = LambdaCFUpdater()
        response = updater.update(
            platform_info=None, image=new_image_uri, timeout_seconds=10
        )

        mock_client.describe_stack_events.assert_called_once_with(
            StackName="cf_stack_id"
        )
        mock_client.describe_stacks.assert_called_with(StackName="cf_stack_id")
        self.assertEqual("UPDATE_COMPLETE", response["status"])
        self.assertEqual(
            [
                {
                    "logical_resource_id": "1",
                    "resource_status": "UPDATE_COMPLETE",
                    "resource_status_reason": "OK",
                    "resource_type": "Bucket",
                    "timestamp": ANY,
                },
                {
                    "logical_resource_id": "2",
                    "resource_status": "UPDATE_COMPLETE",
                    "resource_status_reason": "OK",
                    "resource_type": "Lambda",
                    "timestamp": ANY,
                },
            ],
            response["events"],
        )

    @patch.dict(
        os.environ,
        {
            CLOUDFORMATION_STACK_ID_ENV_VAR: "cf_stack_id",
        },
    )
    @patch("boto3.client")
    def test_update_events_paging(self, mock_boto_client):
        prev_image_uri = (
            "123_account.dkr.ecr.us-east-1.amazonaws.com/repo-name:tag-name"
        )
        new_image_uri = (
            "123_account.dkr.ecr.us-east-1.amazonaws.com/repo-name:new-tag-name"
        )

        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.describe_stacks.return_value = {
            "Stacks": [
                {
                    "Parameters": [
                        {
                            "ParameterKey": "ImageUri",
                            "ParameterValue": prev_image_uri,
                        }
                    ],
                    "StackStatus": "UPDATE_COMPLETE",
                }
            ]
        }
        start_time = datetime.now(timezone.utc)
        mock_client.describe_stack_events.side_effect = [
            {
                "StackEvents": [
                    {
                        "Timestamp": start_time + timedelta(seconds=11),
                        "LogicalResourceId": "1",
                        "ResourceStatus": "UPDATE_COMPLETE",
                        "ResourceType": "Bucket",
                        "ResourceStatusReason": "OK",
                    },
                    {
                        "Timestamp": start_time + timedelta(seconds=10),
                        "LogicalResourceId": "2",
                        "ResourceStatus": "UPDATE_COMPLETE",
                        "ResourceType": "Lambda",
                        "ResourceStatusReason": "OK",
                    },
                ],
                "NextToken": "123",
            },
            {
                "StackEvents": [
                    {
                        "Timestamp": start_time + timedelta(seconds=9),
                        "LogicalResourceId": "3",
                        "ResourceStatus": "UPDATE_COMPLETE",
                        "ResourceType": "Lambda",
                        "ResourceStatusReason": "OK",
                    },
                    {
                        "Timestamp": start_time - timedelta(seconds=1),
                        "LogicalResourceId": "4",
                    },
                ]
            },
        ]
        updater = LambdaCFUpdater()
        response = updater.update(
            platform_info=None, image=new_image_uri, timeout_seconds=10
        )

        mock_client.describe_stack_events.assert_has_calls(
            [
                call(StackName="cf_stack_id"),
                call(StackName="cf_stack_id", NextToken="123"),
            ]
        )
        mock_client.describe_stacks.assert_called_with(StackName="cf_stack_id")
        self.assertEqual("UPDATE_COMPLETE", response["status"])
        self.assertEqual(
            [
                {
                    "logical_resource_id": "1",
                    "resource_status": "UPDATE_COMPLETE",
                    "resource_status_reason": "OK",
                    "resource_type": "Bucket",
                    "timestamp": ANY,
                },
                {
                    "logical_resource_id": "2",
                    "resource_status": "UPDATE_COMPLETE",
                    "resource_status_reason": "OK",
                    "resource_type": "Lambda",
                    "timestamp": ANY,
                },
                {
                    "logical_resource_id": "3",
                    "resource_status": "UPDATE_COMPLETE",
                    "resource_status_reason": "OK",
                    "resource_type": "Lambda",
                    "timestamp": ANY,
                },
            ],
            response["events"],
        )
