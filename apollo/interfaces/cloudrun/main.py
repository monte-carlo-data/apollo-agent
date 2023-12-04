import logging
import os
from typing import Dict, Optional

import google.cloud.logging

from apollo.agent.constants import (
    LOG_ATTRIBUTE_OPERATION_NAME,
    LOG_ATTRIBUTE_TRACE_ID,
)
from apollo.agent.env_vars import DEBUG_LOG_ENV_VAR
from apollo.interfaces.cloudrun.cloudrun_log_context import CloudRunLogContext
from apollo.interfaces.cloudrun.platform import CloudRunPlatformProvider

# CloudRun specific application that adds support for structured logging

# initialize CloudRun logging
gcp_logging_client = google.cloud.logging.Client()
is_debug_log = os.getenv(DEBUG_LOG_ENV_VAR, "false").lower() == "true"
gcp_logging_client.setup_logging(
    log_level=logging.DEBUG if is_debug_log else logging.INFO
)

log_context = CloudRunLogContext()
root_logger = logging.getLogger()
for h in root_logger.handlers:
    h.addFilter(lambda record: log_context.filter(record))

# intentionally imported here to initialize generic main after gcp logging
from apollo.interfaces.generic import main


# CloudRun requires "extra" attributes to be included in a "json_fields" attribute.
# Trace id can be sent along "json_fields" in a "trace" attribute, and it would replace the CloudRun trace id, but
# we're logging it in a separate attribute ("mcd_trace_id" under "json_fields") so we can relate this log message with
# other log messages logged by CloudRun for the same request.
def cloud_run_extra_builder(trace_id: Optional[str], operation_name: str, extra: Dict):
    json_fields = {
        LOG_ATTRIBUTE_OPERATION_NAME: operation_name,
        **extra,
    }
    if trace_id:
        json_fields[LOG_ATTRIBUTE_TRACE_ID] = trace_id

    return {
        "json_fields": json_fields,
    }


main.logging_utils.extra_builder = cloud_run_extra_builder
app = main.app

# set the container platform as GCP for the health endpoint
main.agent.platform_provider = CloudRunPlatformProvider()
main.agent.log_context = log_context
