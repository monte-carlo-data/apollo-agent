import logging
import os
from typing import Dict, Optional

import google.cloud.logging
import requests
from requests import RequestException

from apollo.agent.constants import PLATFORM_GCP
from apollo.interfaces.cloudrun.cloudrun_updater import CloudRunUpdater

# CloudRun specific application that adds support for structured logging

# initialize CloudRun logging
gcp_logging_client = google.cloud.logging.Client()
gcp_logging_client.setup_logging()

logger = logging.getLogger(__name__)

# intentionally imported here to initialize generic main after gcp logging
from apollo.interfaces.generic import main


# CloudRun requires "extra" attributes to be included in a "json_fields" attribute.
# Trace id can be sent along "json_fields" in a "trace" attribute and it would replace the CloudRun trace id, but
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


def _get_metadata(id: str) -> Optional[str]:
    try:
        # https://cloud.google.com/run/docs/container-contract#metadata-server
        url = f"http://metadata.google.internal{id}"
        response = requests.get(url, headers={"Metadata-Flavor": "Google"})
        return response.content.decode("utf-8") if response.content else None
    except RequestException:
        logger.exception(
            f"Failed to get {id} from metadata server, is this running in GCP CloudRun?"
        )


main.logging_utils.extra_builder = cloud_run_extra_builder
app = main.app

# set the container platform as GCP for the health endpoint
main.agent.platform = PLATFORM_GCP
main.agent.platform_info = {
    "service_name": os.getenv(
        "K_SERVICE"
    ),  # https://cloud.google.com/run/docs/container-contract#services-env-vars
    "project-id": _get_metadata("/computeMetadata/v1/project/project-id"),
    "region": _get_metadata("/computeMetadata/v1/instance/region"),
}
main.agent.updater = CloudRunUpdater()
