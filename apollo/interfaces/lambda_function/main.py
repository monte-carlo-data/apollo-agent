import logging
from typing import Tuple, Dict, Optional, cast
from flask import request

from apollo.agent.constants import PLATFORM_AWS
from apollo.agent.utils import AgentUtils
from apollo.interfaces.agent_response import AgentResponse
from apollo.interfaces.generic import main
from apollo.interfaces.lambda_function.cf_platform import CFPlatformProvider

_DEFAULT_LOGS_LIMIT = 1000

app = main.app
agent = main.agent

logger = logging.getLogger(__name__)


def _check_aws_platform(
    trace_id: Optional[str],
) -> Tuple[Optional[CFPlatformProvider], Optional[AgentResponse]]:
    if agent.platform != PLATFORM_AWS:
        return None, AgentUtils.agent_response_for_error(
            "Only supported for AWS platform", trace_id=trace_id
        )
    return cast(CFPlatformProvider, agent.platform_provider), None


@app.route("/api/v1/aws/logs/filter", methods=["GET", "POST"])
def aws_logs_filter() -> Tuple[Dict, int]:
    """
    Uses CloudWatchLogs.filter_log_events API to return a list of log events in the specified time window.
    Documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs/client/filter_log_events.html
    Supported parameters (all optional):
    - trace_id
    - start_time (iso format), defaults to now - 10 minutes
    - end_time (iso format), defaults to now
    - pattern: optional filter pattern, for syntax see:
        https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/FilterAndPatternSyntax.html
    - limit: number of log events to return, defaults to 1,000
    :return: a dictionary with an "events" attribute containing the events returned by CloudWatch, containing
        for example "message" and "timestamp" attributes.
    """
    request_dict: Dict = request.json if request.method == "POST" else request.args  # type: ignore
    trace_id: Optional[str] = request_dict.get("trace_id")
    start_time_str: Optional[str] = request_dict.get("start_time")
    end_time_str: Optional[str] = request_dict.get("end_time")
    pattern: Optional[str] = request_dict.get("pattern")
    limit_str = request_dict.get("limit")

    logger.info(
        "aws/logs/filter requested",
        extra=dict(
            pattern=pattern,
            start_time=start_time_str,
            end_time=end_time_str,
            limit=limit_str,
            mcd_trace_id=trace_id,
        ),
    )
    aws, error_response = _check_aws_platform(trace_id)
    if error_response:
        return error_response.result, error_response.status_code

    try:
        events = (
            aws.filter_log_events(
                pattern,
                start_time_str,
                end_time_str,
                int(limit_str) if limit_str else _DEFAULT_LOGS_LIMIT,
            )
            if aws
            else []
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


@app.route("/api/v1/aws/logs/start_query", methods=["POST"])
def aws_logs_start_query() -> Tuple[Dict, int]:
    return {}, 200


@app.route("/api/v1/aws/logs/query_results", methods=["GET", "POST"])
def aws_logs_get_query_results() -> Tuple[Dict, int]:
    return {}, 200
