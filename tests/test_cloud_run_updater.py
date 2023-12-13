from unittest import TestCase
from unittest.mock import patch, create_autospec, call

from box import Box
from google.api_core.operation import Operation
from google.cloud.run_v2 import Service, ServicesClient, UpdateServiceRequest
from requests import Response

from apollo.interfaces.cloudrun.cloudrun_updater import CloudRunUpdater
from apollo.interfaces.cloudrun.metadata_service import (
    GcpMetadataService,
    GCP_PLATFORM_INFO_KEY_SERVICE_NAME,
    GCP_PLATFORM_INFO_KEY_REGION,
)


class GcpUpdaterTests(TestCase):
    @patch("requests.get")
    def test_metadata(self, requests_get_mock):
        response_mock = create_autospec(Response)
        requests_get_mock.return_value = response_mock

        expected_project_id = "prj-id"
        response_mock.content = expected_project_id.encode("utf-8")
        project_id = GcpMetadataService.get_project_id()
        self.assertEqual(expected_project_id, project_id)

        expected_region = f"projects/{project_id}/regions/us-east2"
        response_mock.content = expected_region.encode("utf-8")
        region = GcpMetadataService.get_instance_region()
        self.assertEqual(expected_region, region)
        requests_get_mock.assert_has_calls(
            [
                call(
                    "http://metadata.google.internal/computeMetadata/v1/project/project-id",
                    headers={"Metadata-Flavor": "Google"},
                ),
                call(
                    "http://metadata.google.internal/computeMetadata/v1/instance/region",
                    headers={"Metadata-Flavor": "Google"},
                ),
            ]
        )

    @patch("apollo.interfaces.cloudrun.cloudrun_updater.run_v2.ServicesClient")
    @patch("apollo.interfaces.cloudrun.cloudrun_updater.run_v2.UpdateServiceRequest")
    def test_updater(self, cloudrun_update_request_mock, cloudrun_client_mock):
        updater = CloudRunUpdater(
            {
                GCP_PLATFORM_INFO_KEY_SERVICE_NAME: "test-agent",
                GCP_PLATFORM_INFO_KEY_REGION: "projects/prj-id/regions/us-east2",
            }
        )

        image_old = "montecarlodata/agent:0.0.8-cloudrun"
        image = "montecarlodata/agent:0.0.9-cloudrun"

        mock_client = create_autospec(ServicesClient)
        cloudrun_client_mock.return_value = mock_client

        # service_mock is the current service
        service_mock = create_autospec(Service)
        mock_client.get_service.return_value = service_mock
        service_mock.latest_ready_revision = "1.2.3"
        service_mock.template = Box(
            containers=[
                {
                    "image": image_old,
                    "env": [
                        {
                            "name": "MCD_AGENT_IMAGE_TAG",
                            "value": image_old,
                        }
                    ],
                }
            ]
        )
        update_request_mock = create_autospec(UpdateServiceRequest)
        cloudrun_update_request_mock.return_value = update_request_mock

        # updated_service is the result for the operation returned by update_service request
        updated_service_mock = create_autospec(Service)
        updated_service_mock.latest_created_revision = "1.2.4"
        updated_service_mock.name = "srv_name"

        operation_mock = create_autospec(Operation)
        mock_client.update_service.return_value = operation_mock
        operation_mock.result.return_value = updated_service_mock

        result = updater.update(image=image, timeout_seconds=20)
        # assert that we updated the image in the service passed to update service
        self.assertEqual(image, service_mock.template.containers[0].image)
        self.assertEqual(image, service_mock.template.containers[0].env[0].value)

        self.assertEqual("1.2.4", result["revision"])
        self.assertEqual("srv_name", result["service-name"])
