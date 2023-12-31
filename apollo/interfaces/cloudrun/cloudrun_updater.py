import logging
from datetime import datetime
from typing import Dict, Optional, cast, List

from google.cloud import run_v2
from google.cloud.run_v2 import Service, EnvVar

from apollo.agent.env_vars import AGENT_IMAGE_TAG_ENV_VAR
from apollo.agent.models import AgentConfigurationError
from apollo.agent.updater import AgentUpdater
from apollo.interfaces.cloudrun.metadata_service import (
    GCP_PLATFORM_INFO_KEY_SERVICE_NAME,
    GCP_PLATFORM_INFO_KEY_REGION,
)

_DEFAULT_TIMEOUT = 5 * 60  # 5 minutes

logger = logging.getLogger(__name__)


class CloudRunUpdater(AgentUpdater):
    def __init__(self, platform_info: Dict):
        """
        :param platform_info: the GCP platform info, loaded when the agent started and used to obtain the service
            name and region that were loaded from the metadata service.
        """
        self._platform_info = platform_info

    """
    Agent updater for CloudRun, it uses `google-cloud-run` API to get the service and update it.
    See https://cloud.google.com/run/docs/reference/rest for docs.
    Permissions required:
    - iam.serviceAccounts.actAs
    - run.operations.get
    - run.services.get
    - run.services.update
    """

    def update(
        self,
        image: Optional[str],
        timeout_seconds: Optional[int],
        **kwargs,  # type: ignore
    ) -> Dict:
        """
        Updates the CloudRun service to the specified image, waits for the operation to complete
        for `timeout_seconds` (defaults to 5 minutes).
        CloudRun Admin API is used to get the service object, if image is specified it is set as the
        `image` attribute in the first container (that is supposed to be the only one) and if `MCD_AGENT_IMAGE_TAG`
        env var is found it's also updated with the same value.
        Then `update_service` from CloudRun Admin API is used to update the service.

        :param image: optional image id, expected format: montecarlodata/repo_name:tag, for example:
            montecarlodata/agent:1.0.1-cloudrun. If not specified, the service is updated without setting the image
            attribute, which is usually ignored by GCP.
        :param timeout_seconds: optional timeout in seconds, default to 5 minutes.
        """
        service = self._get_service(self._platform_info)
        logger.info(
            f"CloudRun service obtained, latest revision={service.latest_ready_revision}"
        )
        timeout_seconds = timeout_seconds or _DEFAULT_TIMEOUT

        if image:
            logger.info(f"CloudRun service, updating image to: {image}")
            service.template.containers[0].image = image

            # update the value for MCD_AGENT_IMAGE_TAG env var
            env = service.template.containers[0].env
            image_env: Optional[EnvVar] = next(
                filter(lambda e: e.name == AGENT_IMAGE_TAG_ENV_VAR, env), None
            )
            if image_env:
                image_env.value = image

        logger.info(
            f"CloudRun service, requesting update with timeout={timeout_seconds}"
        )
        client = run_v2.ServicesClient()
        update_request = run_v2.UpdateServiceRequest()
        update_request.service = service
        update_operation = client.update_service(update_request)
        update_result = cast(Service, update_operation.result(timeout=timeout_seconds))
        logger.info(
            f"CloudRun service, update complete, revision: {update_result.latest_created_revision}"
        )

        return {
            "service-name": update_result.name,
            "revision": update_result.latest_created_revision,
        }

    def get_current_image(self) -> Optional[str]:
        """
        Returns the image currently used by this service, used by the `health` endpoint.
        """
        return self._get_service_image(self._platform_info)

    def get_update_logs(self, start_time: datetime, limit: int) -> List[Dict]:
        return []

    @classmethod
    def _get_service_image(cls, platform_info: Dict) -> Optional[str]:
        """
        Returns the current image used by the service, the information is retrieved from the `image` attribute
        for the first container in the template, that should be the only container for CloudRun services, the
        same attribute that gets updated when the service is upgraded.
        If an exception occurs when retrieving the information, the exception is logged and `None` is returned.
        :param platform_info: the GCP platform info, loaded when the agent started and used to obtain the service
            name and region that were loaded from the metadata service.
        :return: The image currently used by the service, obtained from the `image` attribute
            for the first container in the template
        """
        try:
            service = cls._get_service(platform_info)
            return service.template.containers[0].image
        except Exception:
            logger.exception("Failed to get image attribute for GCP service")
            return None

    @staticmethod
    def _get_service_full_name(service_name: str, region_with_project: str) -> str:
        """
        Returns a string in the format projects/{project-id}/locations/{region}/services/{service-name}
        :param service_name: the service name, for example 'dev-agent'
        :param region_with_project: the region id, including project as returned by metadata service
            for '/computeMetadata/v1/instance/region', in the format:
            projects/{project-numeric-id}/regions/{region}
        :return: the service id in the format expected by GetServiceRequest:
            projects/{project-id}/locations/{region}/services/{service-name}
        """
        prefix = region_with_project.replace("/regions/", "/locations/")
        return f"{prefix}/services/{service_name}"

    @classmethod
    def _get_service(cls, platform_info: Dict) -> Service:
        service_name = platform_info.get(
            GCP_PLATFORM_INFO_KEY_SERVICE_NAME
        )  # service name, like 'dev-agent'
        region = platform_info.get(
            GCP_PLATFORM_INFO_KEY_REGION
        )  # region including project: projects/{project-numeric-id}/regions/{region}

        if not service_name or not region:
            raise AgentConfigurationError(
                "Service name and region are required to update a CloudRun service"
            )

        logger.info(f"CloudRun service lookup, service={service_name}, region={region}")
        client = run_v2.ServicesClient()
        service_full_name = cls._get_service_full_name(service_name, region)
        logger.info(f"CloudRun service full name resolved to {service_full_name}")

        # name is a string with format: projects/{project-id}/locations/{region}/services/{service-name}
        request = run_v2.GetServiceRequest()
        request.name = service_full_name
        return client.get_service(request=request)
