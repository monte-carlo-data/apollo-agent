import os
from typing import Dict, Optional

import google.cloud.logging

from apollo.agent.constants import PLATFORM_GCP
from apollo.interfaces.cloudrun.cloudrun_updater import CloudRunUpdater
from apollo.interfaces.cloudrun.metadata_service import (
    GcpMetadataService,
    GCP_PLATFORM_INFO_KEY_PROJECT_ID,
    GCP_PLATFORM_INFO_KEY_REGION,
    GCP_PLATFORM_INFO_KEY_SERVICE_NAME,
    GCP_ENV_NAME_SERVICE_NAME,
)

# CloudRun specific application that adds support for structured logging

# initialize CloudRun logging
gcp_logging_client = google.cloud.logging.Client()
gcp_logging_client.setup_logging()

# intentionally imported here to initialize generic main after gcp logging
from apollo.interfaces.generic import main


# CloudRun requires "extra" attributes to be included in a "json_fields" attribute.
# Trace id can be sent along "json_fields" in a "trace" attribute, and it would replace the CloudRun trace id, but
# we're logging it in a separate attribute ("mcd_trace_id" under "json_fields") so we can relate this log message with
# other log messages logged by CloudRun for the same request.
def cloud_run_extra_builder(trace_id: Optional[str], operation_name: str, extra: Dict):
    json_fields = {
        "operation_name": operation_name,
        **extra,
    }
    if trace_id:
        json_fields["mcd_trace_id"] = trace_id

    return {
        "json_fields": json_fields,
    }


main.logging_utils.extra_builder = cloud_run_extra_builder
app = main.app

# set the container platform as GCP for the health endpoint
main.agent.platform = PLATFORM_GCP
main.agent.platform_info = {
    GCP_PLATFORM_INFO_KEY_SERVICE_NAME: os.getenv(GCP_ENV_NAME_SERVICE_NAME),
    GCP_PLATFORM_INFO_KEY_PROJECT_ID: GcpMetadataService.get_project_id(),
    GCP_PLATFORM_INFO_KEY_REGION: GcpMetadataService.get_instance_region(),
}

main.agent.updater = CloudRunUpdater()
