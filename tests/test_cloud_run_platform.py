import os
from unittest import TestCase
from unittest.mock import patch, Mock

from apollo.interfaces.cloudrun.metadata_service import GcpMetadataService
from apollo.interfaces.cloudrun.platform import CloudRunPlatformProvider


class TestCloudRunPlatform(TestCase):
    @patch.dict(
        os.environ,
        {"K_SERVICE": "service_name"},
    )
    @patch.object(GcpMetadataService, "get_project_id")
    @patch.object(GcpMetadataService, "get_instance_region")
    def test_platform(self, mock_instance_region, mock_project_id):
        mock_instance_region.return_value = "_region"
        mock_project_id.return_value = "_project_id"
        platform = CloudRunPlatformProvider()

        self.assertEqual("GCP", platform.platform)
        self.assertEqual(
            {
                "project-id": "_project_id",
                "service-name": "service_name",
                "region": "_region",
            },
            platform.platform_info,
        )

        logging_client = Mock()
        logger = Mock()
        logging_client.logger.return_value = logger
        entry_1 = Mock()
        entry_1.to_api_repr.return_value = {
            "message": "abc",
            "timestamp": "123",
        }
        entry_2 = Mock()
        entry_2.to_api_repr.return_value = {
            "message": "xyz",
            "timestamp": "789",
        }
        logger.list_entries.return_value = [
            entry_1,
            entry_2,
        ]
        result = platform.get_gcp_logs(
            logging_client, logs_filter='timestamp >= "2023-12-01"', limit=10
        )
        self.assertEqual(
            [entry_1.to_api_repr.return_value, entry_2.to_api_repr.return_value], result
        )
        expected_filter = (
            f'timestamp >= "2023-12-01"\nresource.type = "cloud_run_revision"\n'
            f'resource.labels.service_name = "service_name"\n'
        )
        logger.list_entries.assert_called_once_with(
            max_results=10, order_by="timestamp asc", filter_=expected_filter
        )
