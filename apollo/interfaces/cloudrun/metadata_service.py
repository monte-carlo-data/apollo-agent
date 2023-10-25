import logging
from typing import Optional

import requests
from requests import RequestException

logger = logging.getLogger(__name__)

GCP_ENV_NAME_SERVICE_NAME = "K_SERVICE"  # https://cloud.google.com/run/docs/container-contract#services-env-vars

GCP_PLATFORM_INFO_KEY_PROJECT_ID = "project-id"
GCP_PLATFORM_INFO_KEY_REGION = "region"
GCP_PLATFORM_INFO_KEY_SERVICE_NAME = "service-name"
GCP_PLATFORM_INFO_KEY_IMAGE = "image"

_METADATA_BASE_URL = "http://metadata.google.internal"
_METADATA_PATH_PROJECT_ID = "/computeMetadata/v1/project/project-id"
_METADATA_PATH_INSTANCE_REGION = "/computeMetadata/v1/instance/region"

_METADATA_FLAVOR_HEADER = "Metadata-Flavor"
_METADATA_FLAVOR_VALUE = "Google"


class GcpMetadataService:
    """
    Service used to obtain metadata from the GCP Metadata Service, more information at:
    https://cloud.google.com/run/docs/container-contract#metadata-server.
    Metadata is obtained performing a GET request to `metadata.google.internal` with a given path, the path
    specifies the requested metadata. The request must include a special header: "Metadata-Flavor: Google".
    """

    @classmethod
    def get_project_id(cls) -> Optional[str]:
        """
        Returns the project-id for the CloudRun function hosting this code, as returned by the metadata service
        for the path: `/computeMetadata/v1/project/project-id`.
        :return: the project-id for the CloudRun function hosting this code, `None` if there was an error calling
            the metadata service, for example because this code is not running in a CloudRun service.
        """
        return cls._get_metadata(_METADATA_PATH_PROJECT_ID)

    @classmethod
    def get_instance_region(cls):
        """
        Returns the region for the CloudRun function hosting this code, as returned by the metadata service
        for the path: `/computeMetadata/v1/instance/region`.
        Please note that the returned value is not only the region id, it also includes the project-id,
        the format for the returned string is: `projects/{project-numeric-id}/regions/{region}`.
        :return: the region for the CloudRun function hosting this code, `None` if there was an error calling
            the metadata service, for example because this code is not running in a CloudRun service.
        """
        return cls._get_metadata(_METADATA_PATH_INSTANCE_REGION)

    @staticmethod
    def _get_metadata(path: str) -> Optional[str]:
        """
        Gets metadata from GCP environment for the given path, more information at:
        https://cloud.google.com/run/docs/container-contract#metadata-server
        :param path: path of the metadata to return, for example: /computeMetadata/v1/project/project-id
        :return: the value returned for the given path from the metadata service and `None` if there was an error
            communicating with the service, most likely because this code is not running in CloudRun.
        """
        try:
            url = f"{_METADATA_BASE_URL}{path}"
            response = requests.get(
                url, headers={_METADATA_FLAVOR_HEADER: _METADATA_FLAVOR_VALUE}
            )
            return response.content.decode("utf-8") if response.content else None
        except RequestException:
            logger.exception(
                f"Failed to get {path} from metadata server, is this running in GCP CloudRun?"
            )
