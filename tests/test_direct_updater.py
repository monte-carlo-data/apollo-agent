import os
from unittest import TestCase
from unittest.mock import patch, Mock, call

from apollo.common.agent.env_vars import AWS_LAMBDA_FUNCTION_NAME_ENV_VAR
from apollo.interfaces.lambda_function.direct_updater import LambdaDirectUpdater


class TestDirectUpdater(TestCase):
    @patch.dict(
        os.environ,
        {
            AWS_LAMBDA_FUNCTION_NAME_ENV_VAR: "test_function",
        },
    )
    @patch("boto3.client")
    def test_get_image_uri(self, mock_boto_client):
        expected_image_uri = (
            "123_account.dkr.ecr.us-east-1.amazonaws.com/repo-name:tag-name"
        )
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_function.return_value = {
            "Code": {
                "ImageUri": expected_image_uri,
            }
        }
        updater = LambdaDirectUpdater()
        uri = updater.get_current_image()
        mock_client.get_function.assert_called_once_with(FunctionName="test_function")
        self.assertEqual(expected_image_uri, uri)

    @patch.dict(
        os.environ,
        {
            AWS_LAMBDA_FUNCTION_NAME_ENV_VAR: "test_function",
            "AWS_REGION": "us-east-1",
        },
    )
    @patch("boto3.client")
    def test_update(self, mock_boto_client):
        prev_image_uri = (
            "123_account.dkr.ecr.us-east-1.amazonaws.com/repo-name:tag-name"
        )
        updated_new_image_uri = (
            "123_account.dkr.ecr.us-east-1.amazonaws.com/repo-name:new-tag-name"
        )
        new_image_uri = "123_account.dkr.ecr.*.amazonaws.com/repo-name:new-tag-name"

        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_function.side_effect = [
            {
                "Code": {
                    "ImageUri": prev_image_uri,
                }
            },
            {
                "Code": {
                    "ImageUri": updated_new_image_uri,
                },
                "Configuration": {
                    "State": "Active",
                },
            },
        ]
        updater = LambdaDirectUpdater()
        response = updater.update(
            image=new_image_uri,
            timeout_seconds=10,
            wait_for_completion=True,
        )

        mock_client.get_function.assert_has_calls(
            [
                call(FunctionName="test_function"),
                call(FunctionName="test_function"),
            ]
        )
        mock_client.update_function_code.assert_called_with(
            FunctionName="test_function", ImageUri=updated_new_image_uri, Publish=True
        )
        self.assertEqual("Active", response["State"])
        self.assertEqual(updated_new_image_uri, response["ImageUri"])

    @patch.dict(
        os.environ,
        {
            AWS_LAMBDA_FUNCTION_NAME_ENV_VAR: "test_function",
            "AWS_REGION": "us-east-1",
        },
    )
    @patch("boto3.client")
    def test_update_no_wait(self, mock_boto_client):
        prev_image_uri = (
            "123_account.dkr.ecr.us-east-1.amazonaws.com/repo-name:tag-name"
        )
        updated_new_image_uri = (
            "123_account.dkr.ecr.us-east-1.amazonaws.com/repo-name:new-tag-name"
        )
        new_image_uri = "123_account.dkr.ecr.*.amazonaws.com/repo-name:new-tag-name"

        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_function.return_value = {
            "Code": {
                "ImageUri": prev_image_uri,
            }
        }
        mock_client.update_function_code.return_value = {
            "State": "Pending",
        }
        updater = LambdaDirectUpdater()
        response = updater.update(
            image=new_image_uri,
            timeout_seconds=10,
        )

        mock_client.get_function.assert_called_once_with(FunctionName="test_function")
        self.assertEqual("Pending", response["State"])
