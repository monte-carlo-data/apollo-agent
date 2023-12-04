import os
from unittest import TestCase
from unittest.mock import patch, Mock

from apollo.agent.agent import Agent
from apollo.agent.constants import ATTRIBUTE_NAME_ERROR, ATTRIBUTE_NAME_RESULT
from apollo.agent.logging_utils import LoggingUtils
from apollo.interfaces.lambda_function.cf_platform import CFPlatformProvider


class TestCFPlatform(TestCase):
    @patch("boto3.client")
    @patch.dict(
        os.environ,
        {
            "MCD_STACK_ID": "arn:stack_id",
        },
    )
    def test_get_infra_details(self, mock_boto_client):
        agent = Agent(LoggingUtils())
        agent.platform_provider = CFPlatformProvider()

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
