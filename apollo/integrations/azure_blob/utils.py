import logging
import os
from typing import Optional

from azure.identity import DefaultAzureCredential

from apollo.common.agent.models import AgentConfigurationError
from azure.mgmt.resource import SubscriptionClient

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

    @classmethod
    def get_subscription_id(cls) -> str:
        owner_name = os.getenv(
            "WEBSITE_OWNER_NAME"
        )  # subscription_id+resource_group_region_etc
        if not owner_name:
            # try getting the subscription id using the default credentials
            subscription_id = cls._get_subscription_id_from_credentials()
            if subscription_id:
                return subscription_id
            raise AgentConfigurationError("Unable to get subscription id")
        if "+" not in owner_name:
            raise AgentConfigurationError(
                f"Unable to get subscription id, invalid owner name: {owner_name}"
            )
        return owner_name.split("+")[0]

    @staticmethod
    def _get_subscription_id_from_credentials() -> Optional[str]:
        client = SubscriptionClient(AzureUtils.get_default_credential())
        subscriptions = list(client.subscriptions.list())
        if not subscriptions:
            logger.error("No subscriptions found using credentials")
            return None
        if len(subscriptions) > 1:
            logger.error("Multiple subscriptions found using credentials")
            return None
        return subscriptions[0].subscription_id
