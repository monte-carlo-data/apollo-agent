import logging
import os
from typing import Optional

from azure.identity import DefaultAzureCredential

from apollo.common.agent.models import AgentConfigurationError

logger = logging.getLogger(__name__)


class AzureUtils:
    """
    Utility methods that return Azure context information like subscription_id, resource_group, etc.
    """

    @staticmethod
    def get_default_credential() -> DefaultAzureCredential:
        # excluding env credential to prevent an annoying warning log message because only AZURE_CLIENT_ID
        # is specified, which we need for user-managed identities.
        return DefaultAzureCredential(exclude_environment_credential=True)

    @staticmethod
    def get_resource_group() -> str:
        resource_group = os.getenv("WEBSITE_RESOURCE_GROUP")
        if not resource_group:
            raise AgentConfigurationError("Unable to get resource group name")
        return resource_group

    @staticmethod
    def get_function_name() -> str:
        function_name = os.getenv("WEBSITE_SITE_NAME", "")
        if not function_name:
            raise AgentConfigurationError("Unable to get function name")
        return function_name

    @staticmethod
    def get_subscription_id() -> str:
        owner_name = os.getenv(
            "WEBSITE_OWNER_NAME"
        )  # subscription_id+resource_group_region_etc
        if not owner_name:
            raise AgentConfigurationError("Unable to get subscription id")
        if "+" not in owner_name:
            raise AgentConfigurationError(
                f"Unable to get subscription id, invalid owner name: {owner_name}"
            )
        return owner_name.split("+")[0]
