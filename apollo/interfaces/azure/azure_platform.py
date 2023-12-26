import json
import logging
from datetime import datetime, timezone, timedelta
from json import JSONDecodeError
from typing import Dict, Optional, List, cast, Any

from azure.monitor.query import (
    LogsQueryClient,
    LogsQueryPartialResult,
    LogsTableRow,
)

from apollo.agent.constants import PLATFORM_AZURE
from apollo.agent.models import AgentConfigurationError
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
        """
        Uses Azure Monitor Query client library to return a list of log events.
        https://learn.microsoft.com/en-us/python/api/overview/azure/monitor-query-readme?view=azure-python
        :param query: a KQL query expression, see https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/.
            If it starts with "traces" or "requests" it is expected to be a "full" query and will be sent "as is", if
            not and not empty it will be assumed to be only the filtering portion of a query
            (like "where message like pattern") and it will be added to the standard query that get traces
            in descending order by timestamp.
        :param start_time_str: start_time (iso format), defaults to now - 10 minutes
        :param end_time_str: end_time (iso format), defaults to now
        :param limit: number of log events to return
        :return: a list of dictionaries containing "message", "customDimensions" and "timestamp" attributes.
        """
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

        resource = AzureUpdater.get_function_resource()

        # get the id for the Application Insights resource used by this function
        resource_id = resource.get("tags", {}).get(
            "hidden-link: /app-insights-resource-id"
        )
        if not resource_id:
            raise AgentConfigurationError("Unable to get app-insights resource-id")

        logger.info(
            "AzurePlatformProvider.get_logs obtained resource id",
            extra={
                "resource_id": resource_id,
            },
        )

        complete_query = None
        if query:
            if query.startswith("traces") or query.startswith("requests"):
                # we assume this is a full query
                complete_query = query
            else:
                # query could be 'where message like "pattern"'
                query = f"traces | {query} |"
        else:
            query = "traces |"
        if not complete_query:
            complete_query = (
                f"{query} project timestamp, message, customDimensions"
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
        data = (
            response.partial_data
            if isinstance(response, LogsQueryPartialResult)
            else response.tables
        )

        rows = data[0].rows if data else []
        columns = data[0].columns if data else []
        return [{key: cls._get_log_value(row, key) for key in columns} for row in rows]

    @staticmethod
    def _get_log_value(row: LogsTableRow, column_name: str) -> Any:
        value = row[column_name]
        if column_name == "customDimensions" and isinstance(value, str):
            try:
                custom_dimensions = json.loads(value)
            except JSONDecodeError:
                return value  # ignore parsing errors
            if "commands" in custom_dimensions:
                try:
                    custom_dimensions["commands"] = json.loads(
                        custom_dimensions["commands"]
                    )
                except JSONDecodeError:
                    pass  # ignore parsing errors
            return custom_dimensions
        elif column_name == "timestamp" and isinstance(value, datetime):
            return value.isoformat()
        return value
