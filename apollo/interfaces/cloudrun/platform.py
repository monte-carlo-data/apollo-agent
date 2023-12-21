import os
from datetime import timezone, timedelta, datetime
from typing import Dict, Optional, List
from google.cloud.logging_v2 import ASCENDING, Client

from apollo.agent.constants import PLATFORM_GCP
from apollo.agent.platform import AgentPlatformProvider
from apollo.agent.updater import AgentUpdater
from apollo.interfaces.cloudrun.cloudrun_updater import CloudRunUpdater
from apollo.interfaces.cloudrun.metadata_service import (
    GCP_PLATFORM_INFO_KEY_SERVICE_NAME,
    GCP_ENV_NAME_SERVICE_NAME,
    GCP_PLATFORM_INFO_KEY_PROJECT_ID,
    GcpMetadataService,
    GCP_PLATFORM_INFO_KEY_REGION,
)

_GCP_DEFAULT_LOGGER = "run.googleapis.com%2Fstderr"


class CloudRunPlatformProvider(AgentPlatformProvider):
    """
    CloudRun platform provider for the agent, returns platform information, the updater and logs.
    """

    def __init__(self):
        self._platform_info = {
            GCP_PLATFORM_INFO_KEY_SERVICE_NAME: os.getenv(GCP_ENV_NAME_SERVICE_NAME),
            GCP_PLATFORM_INFO_KEY_PROJECT_ID: GcpMetadataService.get_project_id(),
            GCP_PLATFORM_INFO_KEY_REGION: GcpMetadataService.get_instance_region(),
        }

    @property
    def platform(self) -> str:
        return PLATFORM_GCP

    @property
    def platform_info(self) -> Dict:
        return self._platform_info

    @property
    def updater(self) -> AgentUpdater:
        return CloudRunUpdater(self._platform_info)

    def get_infra_details(self) -> Dict:
        return {}

    @staticmethod
    def get_gcp_logs(
        gcp_logging_client: Client, logs_filter: Optional[str], limit: int
    ) -> List[Dict]:
        """
        Returns log entries for the current service, if no timestamp filter is specified in "logs_filter" it returns
        the entries for the last 10 minutes.
        It forces the following filters:
            - resource.type = "cloud_run_revision"
            - resource.labels.service_name = <service name>
        Some other filters you can add:
            - severity >= DEFAULT
            - timestamp >= "2023-11-01T13:23:10Z"
            - SEARCH("word1 word2")
        See https://cloud.google.com/logging/docs/view/advanced_filters for more examples.

        Calls "to_api_repr()" on each matched entry to return the same dictionary displayed by the UI.

        Requires: `logging.logEntries.list` permission assigned at the project level to the identity used to run
            the service.
        """
        service_name = os.getenv(GCP_ENV_NAME_SERVICE_NAME)
        logs_filter = f"{logs_filter}\n" if logs_filter else ""
        logs_filter = f'{logs_filter}resource.type = "cloud_run_revision"\n'
        logs_filter = f'{logs_filter}resource.labels.service_name = "{service_name}"\n'
        if "timestamp" not in logs_filter:
            start_time = datetime.now(timezone.utc) - timedelta(minutes=10)
            logs_filter = f'{logs_filter}timestamp >= "{start_time.isoformat()}"'

        gcp_logger = gcp_logging_client.logger(_GCP_DEFAULT_LOGGER)
        events = gcp_logger.list_entries(
            max_results=limit, order_by=ASCENDING, filter_=logs_filter
        )
        return [entry.to_api_repr() for entry in events]
