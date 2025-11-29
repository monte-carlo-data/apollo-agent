import logging
import os
from typing import Dict, Optional, Tuple

import google.cloud.logging
from flask import request

from apollo.common.agent.constants import (
    LOG_ATTRIBUTE_OPERATION_NAME,
    LOG_ATTRIBUTE_TRACE_ID,
)
from apollo.common.agent.env_vars import DEBUG_LOG_ENV_VAR
from apollo.common.agent.utils import AgentUtils
from apollo.interfaces.generic.log_context import BaseLogContext
from apollo.interfaces.cloudrun.platform import CloudRunPlatformProvider

# CloudRun specific application that adds support for structured logging

# initialize CloudRun logging
gcp_logging_client = google.cloud.logging.Client()
is_debug_log = os.getenv(DEBUG_LOG_ENV_VAR, "false").lower() == "true"
gcp_logging_client.setup_logging(
    log_level=logging.DEBUG if is_debug_log else logging.INFO
)

log_context = BaseLogContext(attr_name="json_fields")
log_context.install()

# intentionally imported here to initialize generic main after gcp logging
from apollo.interfaces.generic import main

_DEFAULT_LOGS_LIMIT = 1000
logger = logging.getLogger(__name__)


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

# CloudRun specific endpoints


@app.route("/api/v1/gcp/logs/list", methods=["GET", "POST"])
def gcp_logs_list() -> Tuple[Dict, int]:
    """
    Uses GCP Logs API to return a list of log events.
    Documentation: https://cloud.google.com/logging/docs/reference/libraries
    Supported parameters (all optional):
    - trace_id
    - logs_filter: a filter expression, see https://cloud.google.com/logging/docs/view/advanced_filters,
        if not specified, or specified with a timestamp filter, it adds a filter for the last 10 minutes.
    - limit: number of log events to return, defaults to 1,000
    :return: a dictionary with an "events" attribute containing the events returned by GCP, containing
        for example "jsonPayload" or "textPayload" and "timestamp" attributes.
    """
    request_dict: Dict = request.json if request.method == "POST" else request.args  # type: ignore
    trace_id: Optional[str] = request_dict.get("trace_id")
    limit_str = request_dict.get("limit")
    logs_filter: Optional[str] = request_dict.get("filter")

    logger.info(
        "gcp/logs/list requested",
        extra=main.logging_utils.build_extra(
            trace_id=trace_id,
            operation_name="gcp/logs/list",
            extra=dict(
                filter=logs_filter,
                limit=limit_str,
                mcd_trace_id=trace_id,
            ),
        ),
    )
    try:
        events = CloudRunPlatformProvider.get_gcp_logs(
            gcp_logging_client=gcp_logging_client,
            logs_filter=logs_filter,
            limit=int(limit_str) if limit_str else _DEFAULT_LOGS_LIMIT,
        )
        response = AgentUtils.agent_ok_response(
            {
                "events": events,
            },
            trace_id=trace_id,
        )
    except Exception:
        response = AgentUtils.agent_response_for_last_exception(trace_id=trace_id)

    return response.result, response.status_code
