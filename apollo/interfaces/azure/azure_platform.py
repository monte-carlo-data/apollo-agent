import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List, cast

from azure.monitor.query import LogsQueryClient, LogsQueryStatus, LogsQueryPartialResult

from apollo.agent.constants import PLATFORM_AZURE
from apollo.agent.platform import AgentPlatformProvider
from apollo.agent.updater import AgentUpdater
from apollo.integrations.azure_blob.utils import AzureUtils
from apollo.interfaces.azure.azure_updater import AzureUpdater
from apollo.interfaces.generic.utils import AgentPlatformUtils


logger = logging.getLogger(__name__)


class AzurePlatformProvider(AgentPlatformProvider):
    """
    Azure Platform Provider, uses AzureUpdater to update and return the infra details (that is currently
    returning the Azure Resource for the function.
    """

    @property
    def platform(self) -> str:
        return PLATFORM_AZURE

    @property
    def platform_info(self) -> Dict:
        return {}

    @property
    def updater(self) -> Optional[AgentUpdater]:
        return AzureUpdater()

    def get_infra_details(self) -> Dict:
        return {
            "resource": AzureUpdater.get_function_resource(),
        }

    @classmethod
    def get_logs(
        cls,
        query: Optional[str],
        start_time_str: Optional[str],
        end_time_str: Optional[str],
        limit: int,
    ) -> List[Dict]:
        logger.info("AzurePlatformProvider.get_logs called")
        start_time = cast(
            datetime,
            AgentPlatformUtils.parse_datetime(
                start_time_str, datetime.now(timezone.utc) - timedelta(minutes=10)
            ),
        )
        end_time = cast(
            datetime,
            AgentPlatformUtils.parse_datetime(end_time_str, datetime.now(timezone.utc)),
        )
        logger.info("AzurePlatformProvider.get_logs getting resource id")

        resource_id = cast(str, AzureUpdater.get_function_resource().get("id"))

        logger.info(
            "AzurePlatformProvider.get_logs obtained resource id",
            extra={
                "resource_id": resource_id,
            },
        )

        query_filter = f"| {query}" if query else ""
        complete_query = (
            f"traces {query_filter} | project timestamp, message, customDimensions"
            f"| order by timestamp desc"
            f"| take {limit} "
            f"| order by timestamp asc"
        )

        logs_client = LogsQueryClient(AzureUtils.get_default_credential())
        response = logs_client.query_resource(
            resource_id=resource_id,
            query=complete_query,
            timespan=(start_time, end_time),
        )
        if isinstance(response, LogsQueryPartialResult):
            error = response.partial_error
            data = response.partial_data
        else:
            data = response.tables

        rows = data[0].rows if data else []
        return [dict(row) for row in rows]
