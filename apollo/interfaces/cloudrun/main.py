from typing import Dict

import google.cloud.logging

client = google.cloud.logging.Client()
client.setup_logging()

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
