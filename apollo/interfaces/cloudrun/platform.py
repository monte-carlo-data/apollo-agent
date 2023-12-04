import os
from typing import Dict

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


class CloudRunPlatformProvider(AgentPlatformProvider):
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
