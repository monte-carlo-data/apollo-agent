import logging
from typing import Dict, Optional, cast

from google.cloud import run_v2
from google.cloud.run_v2 import Service

from apollo.agent.models import AgentConfigurationError
from apollo.agent.updater import AgentUpdater

_DEFAULT_TIMEOUT = 5 * 60  # 5 minutes

logger = logging.getLogger(__name__)


class CloudRunUpdater(AgentUpdater):
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
        platform_info: Optional[Dict],
        image: Optional[str],
        timeout_seconds: Optional[int],
        **kwargs,  # type: ignore
    ) -> Dict:
        """
        Updates the CloudRun service to the specified image, waits for the operation to complete
        for `timeout_seconds` (defaults to 5 minutes).
        CloudRun Admin API is used to get the service object, if image is specified it is set as the
        `image` attribute in the first container (that is supposed to be the only one).
        Then `update_service` is used to update the service.
        :param platform_info: the GCP platform info, loaded when the agent started and used to obtain the service
            name and region that were loaded from the metadata service.
        :param image: optional image id, expected format: montecarlodata/repo_name:tag, for example:
            montecarlodata/agent:1.0.1-cloudrun. If not specified, the service is updated without setting the image
            attribute, which is usually ignored by GCP.
        :param timeout_seconds: optional timeout in seconds, default to 5 minutes.
        """
        if not platform_info:
            raise AgentConfigurationError("Platform info missing for CloudRun agent")

        service_name = platform_info.get(
            "service_name"
        )  # service name, like 'dev-agent'
        region = platform_info.get(
            "region"
        )  # region including project: projects/{project-numeric-id}/regions/{region}

        if not service_name or not region:
            raise AgentConfigurationError(
                "Service name and region are required to update a CloudRun service"
            )
        timeout_seconds = timeout_seconds or _DEFAULT_TIMEOUT

        logger.info(
            f"CloudRun update requested, service={service_name}, region={region}"
        )
        client = run_v2.ServicesClient()
        service_full_name = self._get_service_full_name(service_name, region)
        logger.info(f"CloudRun service full name resolved to {service_full_name}")

        # name is a string with format: projects/{project-id}/locations/{region}/services/{service-name}
        request = run_v2.GetServiceRequest()
        request.name = service_full_name
        service = client.get_service(request=request)
        logger.info(
            f"CloudRun service obtained, latest revision={service.latest_ready_revision}"
        )

        if image:
            logger.info(f"CloudRun service, updating image to: {image}")
            service.template.containers[0].image = image

        logger.info(
            f"CloudRun service, requesting update with timeout={timeout_seconds}"
        )
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
