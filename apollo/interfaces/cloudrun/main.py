# Imports the Cloud Logging client library
from typing import Dict

import google.cloud.logging

client = google.cloud.logging.Client()
client.setup_logging()

from apollo.interfaces.generic import main


def cloud_run_extra_builder(trace_id: str, extra: Dict):
    return {
        "json_fields": extra,
        "trace": trace_id,
    }


main.logging_utils.extra_builder = cloud_run_extra_builder
app = main.app
