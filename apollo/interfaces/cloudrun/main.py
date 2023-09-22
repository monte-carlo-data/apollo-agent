from typing import Dict

import google.cloud.logging

# CloudRun specific application that adds support for structured logging

gcp_logging_client = google.cloud.logging.Client()
gcp_logging_client.setup_logging()

from apollo.interfaces.generic import main


# CloudRun requires "extra" attributes to be included in a "json_fields" attribute
# trace can be specified directly and will replace the internal CloudRun trace id
def cloud_run_extra_builder(trace_id: str, operation_name: str, extra: Dict):
    return {
        "json_fields": {
            "operation_name": operation_name,
            **extra,
        },
        "trace": trace_id,
    }


main.logging_utils.extra_builder = cloud_run_extra_builder
app = main.app
main.agent.set_platform_info("GCP")
